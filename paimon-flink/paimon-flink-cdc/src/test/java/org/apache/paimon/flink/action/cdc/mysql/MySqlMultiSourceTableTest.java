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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Source discovery, target safety and actual graph construction for named MySQL sources. */
class MySqlMultiSourceTableTest {

    @TempDir private Path temporary;

    @Test
    void testMergeAndIndependentSourcesWithStableGraph() throws Exception {
        Map<String, String> config = namedConfig();
        Map<String, Schema> schemas = schemas();
        schemas.put(
                "mysql-b",
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("name", DataTypes.VARCHAR(100))
                        .column("note", DataTypes.STRING())
                        .primaryKey("id")
                        .build());
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(2);
            TestingAction action = action(config, schemas, "first");
            action.withStreamExecutionEnvironment(env).build();
            assertThat(action.fileStoreTable().primaryKeys())
                    .containsExactly("source_id", "database_name", "table_name", "id");
            assertThat(action.fileStoreTable().schema().fieldNames())
                    .containsExactly(
                            "id", "name", "note", "source_id", "database_name", "table_name");
            StreamGraph graph = env.getStreamGraph();
            assertThat(graph.getSourceIDs()).hasSize(2);
            Map<String, Integer> parallelism =
                    graph.getStreamNodes().stream()
                            .filter(n -> n.getTransformationUID() != null)
                            .collect(
                                    Collectors.toMap(
                                            StreamNode::getTransformationUID,
                                            StreamNode::getParallelism));
            assertThat(parallelism)
                    .containsEntry("mysql-source-a", 1)
                    .containsEntry("mysql-source-b", 2)
                    .containsEntry("mysql-parser-a", 1)
                    .containsEntry("mysql-parser-b", 2)
                    .containsEntry("mysql-multi-source-route", 2);
            // Validates forward-edge parallelism and serializability, beyond counting config
            // entries.
            graph.getJobGraph();
        }
        List<String> keys = new ArrayList<>(config.keySet());
        Collections.reverse(keys);
        Map<String, String> reversed = new LinkedHashMap<>();
        keys.forEach(k -> reversed.put(k, config.get(k)));
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(2);
            TestingAction action = action(reversed, schemas, "second");
            action.withStreamExecutionEnvironment(env).build();
            assertThat(
                            env.getStreamGraph().getStreamNodes().stream()
                                    .map(StreamNode::getTransformationUID)
                                    .filter(java.util.Objects::nonNull)
                                    .collect(Collectors.toList()))
                    .contains(
                            "mysql-source-a", "mysql-source-b", "mysql-parser-a", "mysql-parser-b");
        }
    }

    @Test
    void testGlobalModeKeepsBusinessPrimaryKey() throws Exception {
        Map<String, String> config = namedConfig();
        config.put("multi-source.key-mode", "global");
        TestingAction action = action(config, schemas(), "global");
        action.build();
        assertThat(action.fileStoreTable().primaryKeys()).containsExactly("id");
        assertThat(action.fileStoreTable().schema().fieldNames()).containsExactly("id", "name");
    }

    @Test
    void testExistingTableWithWrongPrimaryKeyIsRejectedBeforeOptionChanges() throws Exception {
        TestingAction action = action(namedConfig(), schemas(), "existing");
        Schema wrong =
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("name", DataTypes.VARCHAR(20))
                        .column("source_id", DataTypes.STRING())
                        .column("database_name", DataTypes.STRING())
                        .column("table_name", DataTypes.STRING())
                        .primaryKey("id")
                        .option("bucket", "1")
                        .option("snapshot.num-retained.min", "3")
                        .build();
        action.createExisting(wrong);
        action.withTableConfig(Collections.singletonMap("snapshot.num-retained.min", "5"));
        assertThatThrownBy(action::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Primary keys of existing Paimon table")
                .hasMessageContaining("source_id");
        assertThat(action.existingOptions()).containsEntry("snapshot.num-retained.min", "3");
    }

    @Test
    void testSecondInstanceWithoutPrimaryKeyDoesNotCreateTarget() {
        Map<String, Schema> schemas = schemas();
        schemas.put("mysql-b", Schema.newBuilder().column("id", DataTypes.INT()).build());
        TestingAction action = action(namedConfig(), schemas, "nopk");
        assertThatThrownBy(action::build).hasMessageContaining("source 'b'");
        assertThat(action.targetExists()).isFalse();
    }

    @Test
    void testIncompatibleSourceSchemasDoNotCreateTarget() {
        Map<String, Schema> schemas = schemas();
        schemas.put(
                "mysql-b",
                Schema.newBuilder().column("id", DataTypes.DATE()).primaryKey("id").build());
        TestingAction action = action(namedConfig(), schemas, "incompatible");
        assertThatThrownBy(action::build)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("different types")
                .hasMessageContaining("b:app");
        assertThat(action.targetExists()).isFalse();
    }

    @Test
    void testPhysicalMetadataColumnCollisionRejected() {
        Map<String, Schema> schemas = schemas();
        schemas.put(
                "mysql-b",
                Schema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("source_id", DataTypes.STRING())
                        .primaryKey("id")
                        .build());
        TestingAction action = action(namedConfig(), schemas, "collision");
        assertThatThrownBy(action::build).hasMessageContaining("duplicate columns");
        assertThat(action.targetExists()).isFalse();
    }

    @Test
    void testLegacySingleInstanceRetainsGraphAndSchema() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("hostname", "mysql-a");
        config.put("username", "test");
        config.put("password", "test");
        config.put("database-name", "app");
        config.put("table-name", "orders");
        TestingAction action = action(config, schemas(), "legacy");
        try (StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment()) {
            env.setParallelism(1);
            action.withStreamExecutionEnvironment(env).build();
            assertThat(action.fileStoreTable().primaryKeys()).containsExactly("id");
            StreamGraph graph = env.getStreamGraph();
            assertThat(graph.getSourceIDs()).hasSize(1);
            assertThat(
                            graph.getStreamNodes().stream()
                                    .map(StreamNode::getTransformationUID)
                                    .collect(Collectors.toList()))
                    .doesNotContain("mysql-source-default");
        }
    }

    private TestingAction action(
            Map<String, String> config, Map<String, Schema> schemas, String name) {
        TestingAction action =
                new TestingAction(temporary.resolve(name).toString(), config, schemas);
        action.withTableConfig(Collections.singletonMap("bucket", "1"));
        return action;
    }

    private static Map<String, Schema> schemas() {
        Schema schema =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("name", DataTypes.VARCHAR(20))
                        .primaryKey("id")
                        .build();
        Map<String, Schema> result = new HashMap<>();
        result.put("mysql-a", schema);
        result.put("mysql-b", schema);
        return result;
    }

    private static Map<String, String> namedConfig() {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("username", "test");
        config.put("password", "test");
        config.put("database-name", "app");
        config.put("table-name", "orders");
        config.put("server-time-zone", "UTC");
        for (String id : Arrays.asList("b", "a")) {
            config.put("source." + id + ".hostname", "mysql-" + id);
            config.put("source." + id + ".server-id", "5400-5403");
            config.put("source." + id + ".scan.parallelism", "a".equals(id) ? "1" : "2");
        }
        return config;
    }

    private static class TestingAction extends MySqlSyncTableAction {
        private final Map<String, Schema> schemas;

        TestingAction(String warehouse, Map<String, String> config, Map<String, Schema> schemas) {
            super("target", "orders", Collections.singletonMap("warehouse", warehouse), config);
            this.schemas = schemas;
        }

        @Override
        protected JdbcSchemasInfo retrieveSchemas(Configuration config) {
            JdbcSchemasInfo info = new JdbcSchemasInfo();
            info.addSchema(
                    Identifier.create("app", "orders"),
                    schemas.get(config.get(MySqlSourceOptions.HOSTNAME)));
            return info;
        }

        void createExisting(Schema schema) throws Exception {
            catalog.createDatabase("target", true);
            catalog.createTable(Identifier.create("target", "orders"), schema, false);
        }

        Map<String, String> existingOptions() throws Exception {
            return catalog.getTable(Identifier.create("target", "orders")).options();
        }

        boolean targetExists() {
            try {
                catalog.getTable(Identifier.create("target", "orders"));
                return true;
            } catch (Catalog.TableNotExistException e) {
                return false;
            }
        }
    }
}
