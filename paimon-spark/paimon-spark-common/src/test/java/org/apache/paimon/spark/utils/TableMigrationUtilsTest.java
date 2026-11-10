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

package org.apache.paimon.spark.utils;

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

/** Tests for Hive migration through catalog wrappers. */
class TableMigrationUtilsTest {

    @TempDir java.nio.file.Path tempDir;

    @ParameterizedTest
    @EnumSource(CatalogWrapping.class)
    void testHiveTableMigration(CatalogWrapping wrapping) throws Exception {
        IMetaStoreClient client = mock(IMetaStoreClient.class);
        HiveCatalog hiveCatalog = createHiveCatalog(client);

        Migrator migrator =
                TableMigrationUtils.getImporter(
                        "hive",
                        wrapCatalog(hiveCatalog, wrapping),
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
    @EnumSource(CatalogWrapping.class)
    void testHiveDatabaseMigration(CatalogWrapping wrapping) throws Exception {
        IMetaStoreClient client = mock(IMetaStoreClient.class);
        when(client.getAllTables("source_db")).thenReturn(Arrays.asList("first", "second"));
        HiveCatalog hiveCatalog = createHiveCatalog(client);

        List<Migrator> migrators =
                TableMigrationUtils.getImporters(
                        "hive",
                        wrapCatalog(hiveCatalog, wrapping),
                        "source_db",
                        1,
                        Collections.emptyMap());

        assertThat(migrators).hasSize(2).allMatch(migrator -> migrator instanceof HiveMigrator);
        for (Migrator migrator : migrators) {
            migrator.renameTable(false);
        }
        verify(client).getAllTables("source_db");
        for (String table : Arrays.asList("first", "second")) {
            verify(hiveCatalog)
                    .renameTable(
                            Identifier.create("source_db", table + "_paimon_"),
                            Identifier.create("source_db", table),
                            false);
        }
        verify(hiveCatalog, never()).close();
        verify(client, never()).close();
    }

    @ParameterizedTest
    @EnumSource(CatalogWrapping.class)
    void testMissingHiveCatalog(CatalogWrapping wrapping) {
        Catalog catalog =
                wrapCatalog(
                        new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toUri())),
                        wrapping);

        assertMissingHiveCatalog(catalog);
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
                                        "db",
                                        "source",
                                        "db",
                                        "target",
                                        1,
                                        Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only support Hive Catalog");
        assertThatThrownBy(
                        () ->
                                TableMigrationUtils.getImporters(
                                        "hive", catalog, "db", 1, Collections.emptyMap()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only support Hive Catalog");
    }

    @Test
    void testUnsupportedConnector() {
        Catalog catalog = new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toUri()));

        assertThatThrownBy(
                        () ->
                                TableMigrationUtils.getImporter(
                                        "unknown",
                                        catalog,
                                        "db",
                                        "source",
                                        "db",
                                        "target",
                                        1,
                                        Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Unsupported connector unknown");
        assertThatThrownBy(
                        () ->
                                TableMigrationUtils.getImporters(
                                        "unknown", catalog, "db", 1, Collections.emptyMap()))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("Don't support connector unknown");
    }

    private HiveCatalog createHiveCatalog(IMetaStoreClient client) throws Exception {
        HiveCatalog hiveCatalog =
                spy(
                        new HiveCatalog(
                                LocalFileIO.create(),
                                new HiveConf(),
                                HiveMetaStoreClient.class.getName(),
                                tempDir.toUri().toString()));
        doReturn(client).when(hiveCatalog).getHmsClient();
        doNothing().when(hiveCatalog).renameTable(any(), any(), anyBoolean());
        return hiveCatalog;
    }

    private Catalog wrapCatalog(Catalog catalog, CatalogWrapping wrapping) {
        switch (wrapping) {
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
                throw new AssertionError(wrapping);
        }
    }

    private enum CatalogWrapping {
        DIRECT,
        CACHED,
        DELEGATE_AROUND_CACHE,
        CACHE_AROUND_DELEGATE
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
