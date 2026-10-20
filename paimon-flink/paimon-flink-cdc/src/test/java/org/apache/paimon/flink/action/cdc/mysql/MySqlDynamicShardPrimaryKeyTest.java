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
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.sink.cdc.CdcDynamicTableParsingProcessFunction;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.flink.sink.cdc.EventParser;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Regression for PR #2: newly discovered shards must not bypass existing target key validation. */
class MySqlDynamicShardPrimaryKeyTest {

    @TempDir private Path tempDir;

    @ParameterizedTest
    @ValueSource(strings = {"isolated", "global"})
    void testNewShardMappedToExistingTargetIsRejected(String keyMode) throws Exception {
        try (TestingAction action = preparedAction(keyMode)) {
            EventParser<RichCdcMultiplexRecord> parser = action.parserFactory().create();
            parser.setRawEvent(record("app", "orders_1", "code"));
            assertThat(parser.parseTableName()).isEqualTo("orders");
            assertThat(parser.parseNewTable()).isEmpty();
            assertThatThrownBy(parser::parseSchemaChange)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Primary keys")
                    .hasMessageContaining("app.orders_1 -> target.orders");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"isolated", "global"})
    void testEverySchemaEventIsValidatedDespiteNameReuse(String keyMode) throws Exception {
        try (TestingAction action = preparedAction(keyMode)) {
            EventParser<RichCdcMultiplexRecord> parser = action.parserFactory().create();
            parser.setRawEvent(record("app", "orders_1", "id"));
            assertThat(parser.parseNewTable()).isEmpty();
            assertThat(parser.parseSchemaChange()).isNotNull();
            // The same physical name on another instance, and the same table in another
            // database, cannot inherit validation cached for a previously accepted shard.
            for (String db : new String[] {"app", "another_app"}) {
                parser.setRawEvent(record(db, "orders_1", "code"));
                assertThat(parser.parseNewTable()).isEmpty();
                assertThatThrownBy(parser::parseSchemaChange)
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("Primary keys");
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"isolated", "global"})
    void testCompatibleShardAndOrdinaryAddColumnStillWork(String keyMode) throws Exception {
        try (TestingAction action = preparedAction(keyMode)) {
            EventParser<RichCdcMultiplexRecord> parser = action.parserFactory().create();
            RichCdcMultiplexRecord input = record("app", "orders_1", "id");
            CdcSchema.Builder evolved = CdcSchema.newBuilder();
            input.cdcSchema().fields().forEach(evolved::column);
            evolved.column("extra", DataTypes.STRING()).primaryKey("id");
            parser.setRawEvent(
                    new RichCdcMultiplexRecord(
                            "app", "orders_1", evolved.build(), CdcRecord.emptyRecord()));
            assertThat(parser.parseNewTable()).isEmpty();
            assertThat(parser.parseSchemaChange().fields())
                    .extracting(field -> field.name())
                    .contains("extra");
            assertThat(parser.parseRecords()).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"isolated", "global"})
    void testValidationSurvivesFactorySerialization(String keyMode) throws Exception {
        try (TestingAction action = preparedAction(keyMode)) {
            EventParser<RichCdcMultiplexRecord> parser =
                    InstantiationUtil.clone(action.parserFactory()).create();
            parser.setRawEvent(record("app", "orders_1", "code"));
            assertThat(parser.parseNewTable()).isEmpty();
            assertThatThrownBy(parser::parseSchemaChange)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Primary keys");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"isolated", "global"})
    void testExplicitTargetKeyCannotMaskDifferentSourceKey(String keyMode) throws Exception {
        try (TestingAction action = new TestingAction(tempDir, keyMode)) {
            action.withPrimaryKeys("id");
            action.prepare();
            EventParser<RichCdcMultiplexRecord> parser = action.parserFactory().create();
            parser.setRawEvent(record("app", "orders_1", "code"));
            assertThat(parser.parseNewTable()).isEmpty();
            assertThatThrownBy(parser::parseSchemaChange)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Business primary keys");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"isolated", "global"})
    void testConcurrentTargetCreationIsChecked(String keyMode) throws Exception {
        try (TestingAction action = preparedAction(keyMode)) {
            EventParser<RichCdcMultiplexRecord> parser = action.parserFactory().create();
            parser.setRawEvent(record("app", "new_orders", "id"));
            Schema planned = parser.parseNewTable().get();
            // Another subtask creates a different schema after this parser's createdTables
            // snapshot. createTable(ignoreIfExists=true) must not hide the winner's keys.
            action.createDynamicTable(record("app", "new_orders", "code"));
            action.createIfAbsent("new_orders", planned);
            assertThatThrownBy(parser::parseSchemaChange)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Primary keys")
                    .hasMessageContaining("target.new_orders");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"isolated", "global"})
    @SuppressWarnings("unchecked")
    void testOperatorFailsBeforeAnySchemaOrRowOutput(String keyMode) throws Exception {
        try (TestingAction action = preparedAction(keyMode)) {
            CdcDynamicTableParsingProcessFunction<RichCdcMultiplexRecord> function =
                    action.processor();
            ProcessFunction<RichCdcMultiplexRecord, Void>.Context context =
                    mock(ProcessFunction.Context.class);
            Collector<Void> output = mock(Collector.class);
            function.open(new Configuration());
            try {
                assertThatThrownBy(
                                () ->
                                        function.processElement(
                                                record("app", "orders_1", "code"),
                                                context,
                                                output))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("Primary keys");
                verifyNoInteractions(context, output);
            } finally {
                function.close();
            }
        }
    }

    @Test
    void testLegacyParserDoesNotEnableNewValidation() {
        NewTableSchemaBuilder builder =
                new NewTableSchemaBuilder(
                        Collections.emptyMap(),
                        true,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        true,
                        true,
                        Collections.emptyMap(),
                        new CdcMetadataConverter[0]);
        EventParser<RichCdcMultiplexRecord> parser =
                new RichCdcMultiplexRecordEventParser(
                        builder,
                        null,
                        null,
                        null,
                        null,
                        new TableNameConverter(true),
                        new HashSet<>(Collections.singleton("orders")));
        parser.setRawEvent(record("app", "orders", "code"));
        assertThat(parser.parseNewTable()).isEmpty();
        assertThat(parser.parseSchemaChange()).isNotNull();
    }

    private TestingAction preparedAction(String keyMode) throws Exception {
        TestingAction action = new TestingAction(tempDir, keyMode);
        try {
            action.prepare();
            return action;
        } catch (Exception e) {
            action.close();
            throw e;
        }
    }

    private static RichCdcMultiplexRecord record(String db, String table, String key) {
        return new RichCdcMultiplexRecord(
                db,
                table,
                CdcSchema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("code", DataTypes.STRING().notNull())
                        .primaryKey(key)
                        .build(),
                CdcRecord.emptyRecord());
    }

    private static Map<String, String> sourceConfig(String keyMode) {
        Map<String, String> config = new HashMap<>();
        config.put("username", "test");
        config.put("password", "unused");
        config.put("database-name", ".*");
        config.put("server-time-zone", "UTC");
        config.put("source.a.hostname", "mysql-a");
        config.put("source.a.server-id", "5400-5403");
        config.put("source.b.hostname", "mysql-b");
        config.put("source.b.server-id", "6400-6403");
        config.put("multi-source.key-mode", keyMode);
        return config;
    }

    private static class TestingAction extends MySqlSyncDatabaseAction implements AutoCloseable {

        private TestingAction(Path warehouse, String keyMode) {
            super(
                    "target",
                    Collections.singletonMap("warehouse", warehouse.toUri().toString()),
                    sourceConfig(keyMode));
            StreamExecutionEnvironment environment =
                    StreamExecutionEnvironment.getExecutionEnvironment();
            environment.setParallelism(2);
            withStreamExecutionEnvironment(environment);
            withMode(COMBINED);
            Map<String, String> mapping = new HashMap<>();
            mapping.put("orders_0", "orders");
            mapping.put("orders_1", "orders");
            withTableMapping(mapping);
        }

        @Override
        protected JdbcSchemasInfo discoverSource(
                MySqlSourceConfigs.SourceConfig source,
                Predicate<String> tablePredicate,
                List<Identifier> excluded) {
            JdbcSchemasInfo schemas = new JdbcSchemasInfo();
            schemas.addSchema(
                    Identifier.create("app", "orders_0"),
                    record("app", "orders_0", "id").buildSchema());
            return schemas;
        }

        private void prepare() throws Exception {
            validateSourceOptions();
            catalog.createDatabase(database, true);
            beforeBuildingSourceSink();
        }

        private EventParser.Factory<RichCdcMultiplexRecord> parserFactory() {
            return buildEventParserFactory();
        }

        private CdcDynamicTableParsingProcessFunction<RichCdcMultiplexRecord> processor() {
            return new CdcDynamicTableParsingProcessFunction<>(
                    database, catalogLoader(), parserFactory());
        }

        private void createDynamicTable(RichCdcMultiplexRecord record) throws Exception {
            createIfAbsent(record.tableName(), newTableSchemaBuilder().build(record).get());
        }

        private void createIfAbsent(String table, Schema schema) throws Exception {
            catalog.createTable(Identifier.create(database, table), schema, true);
        }

        @Override
        public void close() throws Exception {
            catalog.close();
        }
    }
}
