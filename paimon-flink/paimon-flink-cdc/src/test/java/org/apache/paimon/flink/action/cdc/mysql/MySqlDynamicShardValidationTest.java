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
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.sink.cdc.CdcDynamicTableParsingProcessFunction;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecordEventParser;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/** Regression coverage for the P1 in PR #2: a new shard must not bypass target key checks. */
class MySqlDynamicShardValidationTest {

    private static final Identifier TARGET = Identifier.create("target", "ods_orders");

    @TempDir private Path tempDir;

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExistingTargetRejectsDifferentShardKey(boolean isolated) throws Exception {
        try (Catalog catalog = catalog()) {
            NewTableSchemaBuilder builder = builder(isolated, true);
            createTarget(catalog, builder);
            RichCdcMultiplexRecordEventParser parser = parser(builder, true, true, null);
            parser.setRawEvent(ddl("app", "orders_2", "code", false));

            assertThat(parser.parseNewTable()).isEmpty();
            assertThatThrownBy(() -> parser.validateTableSchema(catalog, TARGET))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("app.orders_2")
                    .hasMessageContaining("code")
                    .hasMessageContaining("target.ods_orders")
                    .hasMessageContaining("id");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testEveryShardAndEveryDdlIsChecked(boolean isolated) throws Exception {
        try (Catalog catalog = catalog()) {
            NewTableSchemaBuilder builder = builder(isolated, true);
            createTarget(catalog, builder);
            RichCdcMultiplexRecordEventParser parser = parser(builder, true, true, null);
            parser.setRawEvent(ddl("app", "orders_1", "id", false));
            parser.validateTableSchema(catalog, TARGET);
            // A second database with the same physical table name must not hit a validation cache.
            parser.setRawEvent(ddl("another_app", "orders_1", "code", false));
            assertThatThrownBy(() -> parser.validateTableSchema(catalog, TARGET))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("another_app.orders_1");
            // ALTER PRIMARY KEY on a previously accepted physical shard is unsafe as well.
            parser.setRawEvent(ddl("app", "orders_1", "code", false));
            assertThatThrownBy(() -> parser.validateTableSchema(catalog, TARGET))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRuntimeCreatedTargetRejectsLaterIncompatibleShard(boolean isolated) throws Exception {
        try (Catalog catalog = catalog()) {
            NewTableSchemaBuilder builder = builder(isolated, true);
            RichCdcMultiplexRecordEventParser parser = parser(builder, true, false, null);
            parser.setRawEvent(ddl("app", "orders_1", "id", false));
            catalog.createTable(TARGET, parser.parseNewTable().get(), true);
            parser.validateTableSchema(catalog, TARGET);

            parser.setRawEvent(ddl("app", "orders_2", "code", false));
            assertThat(parser.parseNewTable()).isEmpty();
            assertThatThrownBy(() -> parser.validateTableSchema(catalog, TARGET))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testFreshParserChecksActualCatalogAfterCreateIfAbsent(boolean isolated) throws Exception {
        try (Catalog catalog = catalog()) {
            NewTableSchemaBuilder builder = builder(isolated, true);
            createTarget(catalog, builder);
            // Models another source winning table creation, or a fresh parser after recovery.
            RichCdcMultiplexRecordEventParser parser = parser(builder, true, false, null);
            parser.setRawEvent(ddl("app", "orders_2", "code", false));
            Schema proposed = parser.parseNewTable().get();
            catalog.createTable(TARGET, proposed, true);

            assertThatThrownBy(() -> parser.validateTableSchema(catalog, TARGET))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(((FileStoreTable) catalog.getTable(TARGET)).primaryKeys())
                    .doesNotContain("code");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompatibleKeysAllowColumnEvolution(boolean isolated) throws Exception {
        try (Catalog catalog = catalog()) {
            NewTableSchemaBuilder builder = builder(isolated, true);
            createTarget(catalog, builder);
            RichCdcMultiplexRecordEventParser parser = parser(builder, true, true, null);
            parser.setRawEvent(ddl("app", "orders_2", "id", true));
            parser.validateTableSchema(catalog, TARGET);

            assertThat(parser.parseSchemaChange().fields())
                    .anyMatch(field -> field.name().equals("extra"));
            assertThat(((FileStoreTable) catalog.getTable(TARGET)).schema().fieldNames())
                    .doesNotContain("extra");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCaseInsensitiveKeys(boolean isolated) throws Exception {
        try (Catalog catalog = catalog()) {
            NewTableSchemaBuilder builder = builder(isolated, false);
            createTarget(catalog, builder);
            RichCdcMultiplexRecordEventParser parser = parser(builder, false, true, null);
            CdcSchema schema =
                    CdcSchema.newBuilder()
                            .column("ID", DataTypes.INT().notNull())
                            .primaryKey(Collections.singletonList("ID"))
                            .build();
            parser.setRawEvent(
                    new RichCdcMultiplexRecord("app", "orders_2", schema, CdcRecord.emptyRecord()));
            parser.validateTableSchema(catalog, TARGET);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionPolicyMustMatchExistingTarget(boolean isolated) throws Exception {
        try (Catalog catalog = catalog()) {
            NewTableSchemaBuilder builder = builder(isolated, true, Arrays.asList("id", "code"));
            Schema source = builder.build(ddl("app", "orders_0", "id", false)).get();
            catalog.createTable(
                    TARGET,
                    new Schema(
                            source.fields(),
                            Collections.singletonList("code"),
                            source.primaryKeys(),
                            source.options(),
                            source.comment()),
                    false);
            RichCdcMultiplexRecordEventParser parser = parser(builder, true, true, null);
            parser.setRawEvent(ddl("app", "orders_2", "id", false));
            assertThatThrownBy(() -> parser.validateTableSchema(catalog, TARGET))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("partition keys");
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testConfiguredKeysCannotHidePhysicalKeyMismatch(boolean isolated) {
        NewTableSchemaBuilder builder =
                MySqlMultiSourceSupport.withPrimaryKeyMode(
                        new NewTableSchemaBuilder(
                                Collections.emptyMap(),
                                true,
                                Collections.emptyList(),
                                Collections.singletonList("id"),
                                true,
                                true,
                                Collections.emptyMap(),
                                metadata(isolated)),
                        isolated);
        assertThatThrownBy(
                        () -> builder.buildForValidation(ddl("app", "orders_2", "code", false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("do not retain")
                .hasMessageContaining("code");
    }

    @Test
    void testFilteredShardDoesNotAccessCatalog() throws Exception {
        RichCdcMultiplexRecordEventParser parser =
                parser(builder(true, true), true, true, Pattern.compile("orders_2"));
        parser.setRawEvent(ddl("app", "orders_2", "code", false));
        Catalog unusedCatalog = mock(Catalog.class);
        parser.validateTableSchema(unusedCatalog, TARGET);
        verifyNoInteractions(unusedCatalog);
        assertThat(parser.parseSchemaChange()).isNull();
        assertThat(parser.parseRecords()).isEmpty();
    }

    @Test
    void testDataOnlyEventDoesNotAccessCatalog() throws Exception {
        RichCdcMultiplexRecordEventParser parser = parser(builder(true, true), true, true, null);
        parser.setRawEvent(
                new RichCdcMultiplexRecord(
                        "app",
                        "orders_2",
                        null,
                        new CdcRecord(RowKind.INSERT, Collections.singletonMap("id", "100"))));
        Catalog unusedCatalog = mock(Catalog.class);
        parser.validateTableSchema(unusedCatalog, TARGET);
        verifyNoInteractions(unusedCatalog);
        assertThat(parser.parseRecords()).hasSize(1);
    }

    @Test
    void testLegacyBuilderDoesNotEnableValidation() throws Exception {
        NewTableSchemaBuilder builder = builder(false, true).withTableSchemaValidation(false);
        RichCdcMultiplexRecordEventParser parser = parser(builder, true, true, null);
        parser.setRawEvent(ddl("app", "orders_2", "code", false));
        Catalog unusedCatalog = mock(Catalog.class);
        parser.validateTableSchema(unusedCatalog, TARGET);
        verifyNoInteractions(unusedCatalog);
        assertThat(parser.parseSchemaChange()).isNotNull();
    }

    @Test
    void testMissingTargetFailsInsteadOfIgnoringValidation() throws Exception {
        try (Catalog catalog = catalog()) {
            RichCdcMultiplexRecordEventParser parser = parser(builder(true, true), true, true, null);
            parser.setRawEvent(ddl("app", "orders_2", "id", false));
            assertThatThrownBy(() -> parser.validateTableSchema(catalog, TARGET))
                    .isInstanceOf(Catalog.TableNotExistException.class);
        }
    }

    @SuppressWarnings("unchecked")
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCombinedProcessRejectsBeforeEitherSideOutput(boolean isolated) throws Exception {
        try (Catalog catalog = catalog()) {
            NewTableSchemaBuilder builder = builder(isolated, true);
            createTarget(catalog, builder);
            CdcDynamicTableParsingProcessFunction<RichCdcMultiplexRecord> function =
                    new CdcDynamicTableParsingProcessFunction<>(
                            TARGET.getDatabaseName(),
                            catalog.catalogLoader(),
                            () -> parser(builder, true, true, null));
            function.open(new Configuration());
            try {
                ProcessFunction<RichCdcMultiplexRecord, Void>.Context context =
                        mock(ProcessFunction.Context.class);
                Collector<Void> collector = mock(Collector.class);
                assertThatThrownBy(
                                () ->
                                        function.processElement(
                                                ddl("app", "orders_2", "code", false),
                                                context,
                                                collector))
                        .isInstanceOf(IllegalArgumentException.class)
                        .hasMessageContaining("incompatible shard");
                verifyNoInteractions(context, collector);
            } finally {
                function.close();
            }
        }
    }

    private Catalog catalog() throws Exception {
        Catalog catalog =
                new FileSystemCatalog(
                        LocalFileIO.create(),
                        new org.apache.paimon.fs.Path(tempDir.toUri().toString()));
        catalog.createDatabase(TARGET.getDatabaseName(), true);
        return catalog;
    }

    private static void createTarget(Catalog catalog, NewTableSchemaBuilder builder)
            throws Exception {
        catalog.createTable(TARGET, builder.build(ddl("app", "orders_0", "id", false)).get(), false);
    }

    private static NewTableSchemaBuilder builder(boolean isolated, boolean caseSensitive) {
        return builder(isolated, caseSensitive, Collections.emptyList());
    }

    private static NewTableSchemaBuilder builder(
            boolean isolated, boolean caseSensitive, List<String> primaryKeys) {
        return MySqlMultiSourceSupport.withPrimaryKeyMode(
                new NewTableSchemaBuilder(
                        Collections.emptyMap(),
                        caseSensitive,
                        Collections.emptyList(),
                        primaryKeys,
                        true,
                        true,
                        Collections.emptyMap(),
                        metadata(isolated)),
                isolated);
    }

    private static CdcMetadataConverter[] metadata(boolean isolated) {
        return isolated
                ? new CdcMetadataConverter[] {
                    new CdcMetadataConverter.SourceIdConverter("source_a"),
                    new CdcMetadataConverter.DatabaseNameConverter(),
                    new CdcMetadataConverter.TableNameConverter()
                }
                : new CdcMetadataConverter[0];
    }

    private static RichCdcMultiplexRecordEventParser parser(
            NewTableSchemaBuilder builder,
            boolean caseSensitive,
            boolean targetExists,
            Pattern excluding) {
        Map<String, String> mapping = new HashMap<>();
        for (String shard : Arrays.asList("orders_0", "orders_1", "orders_2")) {
            mapping.put(shard, TARGET.getObjectName());
        }
        return new RichCdcMultiplexRecordEventParser(
                builder,
                null,
                excluding,
                null,
                null,
                new TableNameConverter(caseSensitive, true, "ods_", "", mapping),
                targetExists
                        ? new HashSet<>(Collections.singleton(TARGET.getObjectName()))
                        : new HashSet<>());
    }

    private static RichCdcMultiplexRecord ddl(
            String database, String table, String key, boolean extraColumn) {
        CdcSchema.Builder schema =
                CdcSchema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("code", DataTypes.STRING().notNull())
                        .column("value", DataTypes.VARCHAR(extraColumn ? 100 : 10));
        if (extraColumn) {
            schema.column("extra", DataTypes.BIGINT());
        }
        return new RichCdcMultiplexRecord(
                database,
                table,
                schema.primaryKey(Collections.singletonList(key)).build(),
                CdcRecord.emptyRecord());
    }
}
