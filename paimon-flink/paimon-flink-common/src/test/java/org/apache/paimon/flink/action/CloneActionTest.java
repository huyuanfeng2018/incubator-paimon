/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.action;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.options.Options;

import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.Map;

import static org.apache.paimon.flink.FlinkCatalogOptions.DISABLE_CREATE_TABLE_IN_DEFAULT_DB;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/** Tests Hive clone source validation through catalog wrappers. */
class CloneActionTest {

    @TempDir java.nio.file.Path warehouse;

    @ParameterizedTest
    @EnumSource(Wrapping.class)
    void testHiveSourceCatalog(Wrapping wrapping) throws Exception {
        HiveCatalog hiveCatalog =
                spy(
                        new HiveCatalog(
                                LocalFileIO.create(),
                                new HiveConf(),
                                HiveMetaStoreClient.class.getName(),
                                warehouse.toUri().toString()));
        Catalog sourceCatalog = wrapping.wrap(hiveCatalog);

        CloneAction action = createAction(sourceCatalog, "hive", Collections.emptyMap());
        action.build();

        assertThat(action.env.getStreamGraph().getStreamNodes())
                .extracting(StreamNode::getOperatorName)
                .contains("Clone Schema", "Copy Splits", "Copy Files");
        assertThat(action.catalog).isSameAs(sourceCatalog);
        assertThat(action.flinkCatalog.catalog()).isSameAs(sourceCatalog);
        verify(hiveCatalog, never()).close();
        verify(hiveCatalog, never()).getHmsClient();
    }

    @ParameterizedTest
    @EnumSource(Wrapping.class)
    void testMissingHiveCatalog(Wrapping wrapping) {
        Catalog sourceCatalog = wrapping.wrap(fileSystemCatalog());
        String expectedType =
                wrapping == Wrapping.DIRECT || wrapping == Wrapping.CACHED
                        ? FileSystemCatalog.class.getName()
                        : TestDelegateCatalog.class.getName();

        assertThatThrownBy(() -> createAction(sourceCatalog, "hive", Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Only support clone hive tables using HiveCatalog, but current source catalog is "
                                + expectedType);
    }

    @Test
    void testHiveCloneRejectsTargetTableOptions() {
        assertThatThrownBy(
                        () ->
                                createAction(
                                        fileSystemCatalog(),
                                        "hive",
                                        Collections.singletonMap("bucket", "-1")))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Parameter 'target_table_conf' is only supported when clone_from is paimon.");
    }

    @Test
    void testPaimonCloneDoesNotRequireHiveCatalog() {
        Catalog sourceCatalog = new TestDelegateCatalog(fileSystemCatalog());

        CloneAction action = createAction(sourceCatalog, "paimon", Collections.emptyMap());

        assertThat(action.catalog).isSameAs(sourceCatalog);
    }

    private FileSystemCatalog fileSystemCatalog() {
        return new FileSystemCatalog(LocalFileIO.create(), new Path(warehouse.toUri()));
    }

    private CloneAction createAction(
            Catalog sourceCatalog, String cloneFrom, Map<String, String> targetTableOptions) {
        try (MockedStatic<CatalogFactory> factory = mockStatic(CatalogFactory.class)) {
            factory.when(() -> CatalogFactory.createCatalog(any(CatalogContext.class)))
                    .thenReturn(sourceCatalog);

            CloneAction action =
                    new CloneAction(
                            "source_db",
                            "source_table",
                            Collections.singletonMap(
                                    DISABLE_CREATE_TABLE_IN_DEFAULT_DB.key(), "true"),
                            "target_db",
                            "target_table",
                            Collections.emptyMap(),
                            targetTableOptions,
                            1,
                            null,
                            null,
                            null,
                            null,
                            cloneFrom,
                            false,
                            false);
            factory.verify(() -> CatalogFactory.createCatalog(any(CatalogContext.class)));
            factory.verifyNoMoreInteractions();
            return action;
        }
    }

    private enum Wrapping {
        DIRECT,
        CACHED,
        DELEGATE_AROUND_CACHE,
        CACHE_AROUND_DELEGATE;

        private Catalog wrap(Catalog catalog) {
            switch (this) {
                case DIRECT:
                    return catalog;
                case CACHED:
                    return new CachingCatalog(catalog, new Options());
                case DELEGATE_AROUND_CACHE:
                    return new TestDelegateCatalog(
                            new CachingCatalog(new TestDelegateCatalog(catalog), new Options()));
                case CACHE_AROUND_DELEGATE:
                    return new CachingCatalog(new TestDelegateCatalog(catalog), new Options());
                default:
                    throw new IllegalStateException("Unknown wrapping: " + this);
            }
        }
    }

    private static class TestDelegateCatalog extends DelegateCatalog {

        private TestDelegateCatalog(Catalog wrapped) {
            super(wrapped);
        }

        @Override
        public CatalogLoader catalogLoader() {
            return wrapped.catalogLoader();
        }
    }
}
