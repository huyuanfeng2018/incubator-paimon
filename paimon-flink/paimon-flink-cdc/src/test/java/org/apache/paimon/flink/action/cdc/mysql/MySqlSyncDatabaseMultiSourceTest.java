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

package org.apache.paimon.flink.action.cdc.mysql;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SerializablePredicate;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests named database-source discovery and target schema validation without external services. */
class MySqlSyncDatabaseMultiSourceTest {

    @TempDir private Path tempDir;

    @Test
    void testMergeSchemasAcrossIndependentInstances() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders", ordersSchema(10));
            action.source(
                    "b",
                    "orders",
                    Schema.newBuilder()
                            .column("id", DataTypes.INT().notNull())
                            .column("value", DataTypes.VARCHAR(100))
                            .column("extra", DataTypes.BIGINT())
                            .primaryKey("id")
                            .build());

            action.prepare();

            FileStoreTable table = action.table("orders");
            assertThat(table.schema().fieldNames())
                    .containsExactly(
                            "id", "value", "extra", "source_id", "database_name", "table_name");
            assertThat(table.schema().fields().get(1).type()).isEqualTo(DataTypes.VARCHAR(100));
            assertThat(table.primaryKeys())
                    .containsExactly("source_id", "database_name", "table_name", "id");
            assertThat(action.monitoredTables("a"))
                    .containsExactly(Identifier.create("app", "orders"));
            assertThat(action.monitoredTables("b"))
                    .containsExactly(Identifier.create("app", "orders"));
            assertThat(action.tableNames()).containsExactly("orders");
        }
    }

    @Test
    void testSourceExclusionsDoNotAffectAnotherInstance() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders", ordersSchema(10));
            action.source("a", "audit", ordersSchema(10));
            action.source("b", "orders", ordersSchema(10));
            action.source(
                    "b",
                    "audit",
                    Schema.newBuilder().column("message", DataTypes.STRING()).build());

            action.prepare();

            Identifier audit = Identifier.create("app", "audit");
            assertThat(action.monitoredTables("a")).contains(audit);
            assertThat(action.excludedTables("a")).doesNotContain(audit);
            assertThat(action.monitoredTables("b")).doesNotContain(audit);
            assertThat(action.excludedTables("b")).contains(audit);
            assertThat(action.tableNames()).containsExactlyInAnyOrder("orders", "audit");
            assertThat(action.sourceTableFilter("a").test(audit)).isTrue();
            assertThat(action.sourceTableFilter("b").test(audit)).isFalse();
        }
    }

    @Test
    void testIncompatibleSourceSchemaDoesNotCreateAnyTargetTable() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders", ordersSchema(10));
            action.source("a", "z_conflict", ordersSchema(10));
            action.source("b", "orders", ordersSchema(10));
            action.source(
                    "b",
                    "z_conflict",
                    Schema.newBuilder()
                            .column("id", DataTypes.INT().notNull())
                            .column("value", DataTypes.INT())
                            .primaryKey("id")
                            .build());

            assertThatThrownBy(action::prepare)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("a:app.z_conflict")
                    .hasMessageContaining("b:app.z_conflict");
            assertThat(action.tableNames()).isEmpty();
        }
    }

    @Test
    void testExistingPrimaryKeyMismatchDoesNotCreateOtherTables() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders", ordersSchema(10));
            action.source("a", "another", ordersSchema(10));
            action.source("b", "orders", ordersSchema(10));
            action.createTable("orders", ordersSchema(10));

            assertThatThrownBy(action::prepare)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Primary keys")
                    .hasMessageContaining("source_id");
            assertThat(action.tableNames()).containsExactly("orders");
            assertThat(action.table("orders").primaryKeys()).containsExactly("id");
        }
    }

    @Test
    void testIgnoreIncompatibleExcludesTheWholeTargetGroup() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders", ordersSchema(10));
            action.source("a", "good", ordersSchema(10));
            action.source("b", "orders", ordersSchema(10));
            action.source("b", "good", ordersSchema(10));
            action.createTable("orders", ordersSchema(10));
            action.ignoreIncompatible(true);
            action.withTableMapping(Collections.singletonMap("orders_1", "orders"));

            action.prepare();

            Identifier orders = Identifier.create("app", "orders");
            Identifier good = Identifier.create("app", "good");
            for (String source : new String[] {"a", "b"}) {
                assertThat(action.monitoredTables(source)).containsExactly(good);
                assertThat(action.excludedTables(source)).containsExactly(orders);
                SerializablePredicate<Identifier> filter =
                        InstantiationUtil.clone(action.sourceTableFilter(source));
                assertThat(filter.test(orders)).isFalse();
                // This physical shard did not exist at startup, but its target was rejected.
                assertThat(filter.test(Identifier.create("app", "orders_1"))).isFalse();
                assertThat(filter.test(good)).isTrue();
                assertThat(filter.test(Identifier.create("app", "new_target"))).isTrue();
            }
            assertThat(action.table("orders").primaryKeys()).containsExactly("id");
            assertThat(action.table("good").primaryKeys())
                    .containsExactly("source_id", "database_name", "table_name", "id");
        }
    }

    @Test
    void testSourceWithoutMatchingPrimaryKeyTableFailsBeforeCreation() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders", ordersSchema(10));
            action.source(
                    "b",
                    "audit",
                    Schema.newBuilder().column("message", DataTypes.STRING()).build());

            assertThatThrownBy(action::prepare)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("source 'b'");
            assertThat(action.tableNames()).isEmpty();
        }
    }

    @Test
    void testDiscoveryFailureIdentifiesTheSourceAndDoesNotCreateTables() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders", ordersSchema(10));
            action.unavailableSource = "b";

            assertThatThrownBy(action::prepare)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Cannot discover MySQL source 'b'")
                    .hasCauseInstanceOf(SQLException.class);
            assertThat(action.tableNames()).isEmpty();
        }
    }

    @Test
    void testGlobalPrimaryKeyModeDoesNotAddIdentityColumns() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "global")) {
            action.source("a", "orders", ordersSchema(10));
            action.source("b", "orders", ordersSchema(10));

            action.prepare();

            assertThat(action.table("orders").primaryKeys()).containsExactly("id");
            assertThat(action.table("orders").schema().fieldNames()).containsExactly("id", "value");
        }
    }

    @Test
    void testMappedShardTablesAreMergedBeforeCreation() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders_0", ordersSchema(10));
            action.source("b", "orders_1", ordersSchema(100));
            Map<String, String> mapping = new HashMap<>();
            mapping.put("orders_0", "orders");
            mapping.put("orders_1", "orders");
            action.withTableMapping(mapping);

            action.prepare();

            assertThat(action.tableNames()).containsExactly("orders");
            assertThat(action.table("orders").schema().fields().get(1).type())
                    .isEqualTo(DataTypes.VARCHAR(100));
        }
    }

    @Test
    void testNewRuntimeTableUsesTheSameIsolatedPrimaryKeyMode() throws Exception {
        try (TestingAction action = new TestingAction(tempDir, "isolated")) {
            action.source("a", "orders", ordersSchema(10));
            action.source("b", "orders", ordersSchema(10));
            action.prepare();
            CdcSchema schema =
                    CdcSchema.newBuilder()
                            .column("id", DataTypes.INT().notNull())
                            .primaryKey(Collections.singletonList("id"))
                            .build();

            Schema dynamic =
                    action.dynamicSchema(
                            new RichCdcMultiplexRecord(
                                    "app", "new_orders", schema, CdcRecord.emptyRecord()));

            assertThat(dynamic.primaryKeys())
                    .containsExactly("source_id", "database_name", "table_name", "id");
        }
    }

    @Test
    void testNamedSourcesRejectUnsupportedRoutingAndAppendOnlyMode() throws Exception {
        try (TestingAction action = new TestingAction(tempDir.resolve("unmerged"), "isolated")) {
            action.mergeShards(false);
            assertThatThrownBy(action::validateSourceOptions)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("merge_shards");
        }
        try (TestingAction action = new TestingAction(tempDir.resolve("append"), "isolated")) {
            action.syncPKeysFromSourceSchema(false);
            assertThatThrownBy(action::validateSourceOptions)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("sync_primary_keys_from_source_schema");
        }
        try (TestingAction action = new TestingAction(tempDir.resolve("prefix"), "isolated")) {
            action.withDbPrefix(Collections.singletonMap("app", "prefix_"));
            assertThatThrownBy(action::validateSourceOptions)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("table_prefix_db");
        }
        try (TestingAction action = new TestingAction(tempDir.resolve("computed"), "isolated")) {
            action.withComputedColumnArgs(Collections.singletonList("constant=cast(1,INT)"));
            assertThatThrownBy(action::validateSourceOptions)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("computed_column");
        }
    }

    private static Schema ordersSchema(int length) {
        return Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("value", DataTypes.VARCHAR(length))
                .primaryKey("id")
                .build();
    }

    private static Map<String, String> sourceConfig(String keyMode) {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("username", "test");
        config.put("password", "unused");
        config.put("database-name", "app");
        config.put("server-time-zone", "UTC");
        config.put("source.b.hostname", "mysql-b");
        config.put("source.b.server-id", "6400-6403");
        config.put("source.a.hostname", "mysql-a");
        config.put("source.a.server-id", "5400-5403");
        config.put("multi-source.key-mode", keyMode);
        return config;
    }

    private static class TestingAction extends MySqlSyncDatabaseAction implements AutoCloseable {

        private final Map<String, Map<Identifier, Schema>> sourceTables = new LinkedHashMap<>();
        private String unavailableSource;

        private TestingAction(Path warehouse, String keyMode) {
            super(
                    "target",
                    Collections.singletonMap("warehouse", warehouse.toUri().toString()),
                    sourceConfig(keyMode));
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            environment.setParallelism(2);
            withStreamExecutionEnvironment(environment);
        }

        private void source(String id, String table, Schema schema) {
            sourceTables
                    .computeIfAbsent(id, ignored -> new LinkedHashMap<>())
                    .put(Identifier.create("app", table), schema);
        }

        @Override
        protected JdbcSchemasInfo discoverSource(
                MySqlSourceConfigs.SourceConfig source,
                Predicate<String> tablePredicate,
                List<Identifier> excluded)
                throws Exception {
            if (source.id().equals(unavailableSource)) {
                throw new SQLException("Connection unavailable in test");
            }
            JdbcSchemasInfo schemas = new JdbcSchemasInfo();
            sourceTables
                    .getOrDefault(source.id(), Collections.emptyMap())
                    .forEach(
                            (table, schema) -> {
                                if (tablePredicate.test(table.getObjectName())) {
                                    schemas.addSchema(table, schema);
                                } else {
                                    excluded.add(table);
                                }
                            });
            return schemas;
        }

        private void prepare() throws Exception {
            validateSourceOptions();
            catalog.createDatabase(database, true);
            beforeBuildingSourceSink();
        }

        private FileStoreTable table(String table) throws Exception {
            return (FileStoreTable) catalog.getTable(Identifier.create(database, table));
        }

        private List<String> tableNames() throws Exception {
            return catalog.listTables(database);
        }

        private void createTable(String table, Schema schema) throws Exception {
            catalog.createDatabase(database, true);
            catalog.createTable(Identifier.create(database, table), schema, false);
        }

        private Schema dynamicSchema(RichCdcMultiplexRecord record) {
            return newTableSchemaBuilder().build(record).get();
        }

        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }
}
