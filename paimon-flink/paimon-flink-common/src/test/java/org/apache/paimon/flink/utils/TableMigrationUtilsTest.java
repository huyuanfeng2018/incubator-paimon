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

package org.apache.paimon.flink.utils;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.DelegateCatalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.migrate.HiveMigrator;
import org.apache.paimon.migrate.Migrator;
import org.apache.paimon.options.Options;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests Hive migration through catalog wrappers. */
class TableMigrationUtilsTest {

    @TempDir java.nio.file.Path warehouse;

    private HiveCatalog hiveCatalog;
    private IMetaStoreClient client;

    @BeforeEach
    void setUp() throws Exception {
        client = mock(IMetaStoreClient.class);
        hiveCatalog =
                spy(
                        new HiveCatalog(
                                LocalFileIO.create(),
                                new HiveConf(),
                                HiveMetaStoreClient.class.getName(),
                                warehouse.toUri().toString()));
        doReturn(client).when(hiveCatalog).getHmsClient();
        doNothing().when(hiveCatalog).renameTable(any(), any(), anyBoolean());
    }

    @ParameterizedTest
    @EnumSource(Wrapping.class)
    void testHiveTableMigration(Wrapping wrapping) throws Exception {
        Migrator migrator =
                TableMigrationUtils.getImporter(
                        "hive",
                        wrapping.wrap(hiveCatalog),
                        "source_db",
                        "source_table",
                        "target_db",
                        "target_table",
                        1,
                        Collections.emptyMap());

        assertThat(migrator).isInstanceOf(HiveMigrator.class);
        migrator.renameTable(false);
        verify(hiveCatalog)
                .renameTable(
                        Identifier.create("target_db", "target_table"),
                        Identifier.create("source_db", "source_table"),
                        false);
        verify(hiveCatalog, never()).close();
        verify(client, never()).close();
    }

    @ParameterizedTest
    @EnumSource(Wrapping.class)
    void testHiveDatabaseMigration(Wrapping wrapping) throws Exception {
        when(client.getAllTables("source_db")).thenReturn(Arrays.asList("first", "second"));

        List<Migrator> migrators =
                TableMigrationUtils.getImporters(
                        "hive", wrapping.wrap(hiveCatalog), "source_db", 1, Collections.emptyMap());

        assertThat(migrators).hasSize(2).allMatch(HiveMigrator.class::isInstance);
        for (Migrator migrator : migrators) {
            migrator.renameTable(false);
        }
        verify(client).getAllTables("source_db");
        verify(hiveCatalog)
                .renameTable(
                        Identifier.create("source_db", "first_paimon_"),
                        Identifier.create("source_db", "first"),
                        false);
        verify(hiveCatalog)
                .renameTable(
                        Identifier.create("source_db", "second_paimon_"),
                        Identifier.create("source_db", "second"),
                        false);
        verify(hiveCatalog, never()).close();
        verify(client, never()).close();
    }

    @ParameterizedTest
    @EnumSource(Wrapping.class)
    void testMissingHiveCatalog(Wrapping wrapping) {
        assertMissingHiveCatalog(
                wrapping.wrap(
                        new FileSystemCatalog(LocalFileIO.create(), new Path(warehouse.toUri()))));
    }

    @Test
    void testNullCatalog() {
        assertMissingHiveCatalog(null);
    }

    private void assertMissingHiveCatalog(Catalog catalog) {
        assertThatThrownBy(
                        () ->
                                TableMigrationUtils.getImporter(
                                        "hive",
                                        catalog,
                                        "source_db",
                                        "source_table",
                                        "target_db",
                                        "target_table",
                                        1,
                                        Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only support Hive Catalog");
        assertThatThrownBy(
                        () ->
                                TableMigrationUtils.getImporters(
                                        "hive", catalog, "source_db", 1, Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only support Hive Catalog");
    }

    @Test
    void testUnsupportedConnector() {
        assertThatThrownBy(
                        () ->
                                TableMigrationUtils.getImporter(
                                        "unknown",
                                        hiveCatalog,
                                        "source_db",
                                        "source_table",
                                        "target_db",
                                        "target_table",
                                        1,
                                        Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Don't support connector unknown");
        assertThatThrownBy(
                        () ->
                                TableMigrationUtils.getImporters(
                                        "unknown",
                                        hiveCatalog,
                                        "source_db",
                                        1,
                                        Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Don't support connector unknown");
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
