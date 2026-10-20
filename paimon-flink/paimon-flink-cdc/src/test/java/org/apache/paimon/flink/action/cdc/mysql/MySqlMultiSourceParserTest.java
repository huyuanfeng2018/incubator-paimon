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
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises source isolation with Debezium row and schema events. */
class MySqlMultiSourceParserTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void testIdentityIsAddedToBothBeforeAndAfterRows() throws Exception {
        for (String id : new String[] {"shard_a", "shard_b"}) {
            List<RichCdcMultiplexRecord> records =
                    parse(parser(id, "UTC", true), row("app", "id", null, true));
            assertThat(records).hasSize(2);
            assertThat(records.get(0).toRichCdcRecord().kind()).isEqualTo(RowKind.DELETE);
            assertThat(records.get(1).toRichCdcRecord().kind()).isEqualTo(RowKind.INSERT);
            for (RichCdcMultiplexRecord record : records) {
                CdcRecord data = record.toRichCdcRecord().toCdcRecord();
                assertThat(data.data())
                        .containsEntry("id", "1")
                        .containsEntry("source_id", id)
                        .containsEntry("database_name", "app")
                        .containsEntry("table_name", "orders");
            }
        }
    }

    @Test
    void testEachSourceUsesItsOwnTimeZone() throws Exception {
        ObjectNode event = row("app", "updated_at", "io.debezium.time.ZonedTimestamp", false);
        ((ObjectNode) event.path("payload").path("after"))
                .put("updated_at", "2026-10-19T00:00:00Z");
        Map<String, String> utc =
                parse(parser("a", "UTC", true), event)
                        .get(0)
                        .toRichCdcRecord()
                        .toCdcRecord()
                        .data();
        Map<String, String> shanghai =
                parse(parser("b", "Asia/Shanghai", true), event)
                        .get(0)
                        .toRichCdcRecord()
                        .toCdcRecord()
                        .data();
        assertThat(utc.get("updated_at")).startsWith("2026-10-19 00:00:00");
        assertThat(shanghai.get("updated_at")).startsWith("2026-10-19 08:00:00");
    }

    @Test
    void testRuntimeRowMetadataCollisionIsRejected() {
        assertThatThrownBy(
                        () ->
                                parse(
                                        parser("a", "UTC", false),
                                        row("app", "SOURCE_ID", null, false)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("conflicts with CDC metadata");
    }

    @Test
    void testRuntimeDdlMetadataCollisionIsRejected() {
        assertThatThrownBy(
                        () ->
                                parse(
                                        parser("a", "UTC", true),
                                        ddl("app", "ALTER", true, "source_id")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source_id");
    }

    @Test
    void testNonPrimaryKeyTableDoesNotSuppressSameNameInAnotherDatabase() throws Exception {
        MySqlRecordParser parser = parser("a", "UTC", true);
        assertThat(parse(parser, ddl("ignored_db", "CREATE", false, "id"))).isEmpty();
        assertThat(parse(parser, row("ignored_db", "id", null, false))).isEmpty();
        assertThat(parse(parser, row("valid_db", "id", null, false))).hasSize(1);
    }

    @Test
    void testTableKeyAlterFailsBeforeAnySchemaEventIsCollected() throws Exception {
        MySqlRecordParser guarded = guardedParser();
        assertThat(parse(guarded, ddl("app", "ALTER", true, "id"))).hasSize(1);
        List<RichCdcMultiplexRecord> output = new ArrayList<>();
        Collector<RichCdcMultiplexRecord> collector = new Collector<RichCdcMultiplexRecord>() {
            @Override
            public void collect(RichCdcMultiplexRecord record) { output.add(record); }
            @Override
            public void close() {}
        };
        assertThatThrownBy(
                        () -> guarded.flatMap(
                                new CdcSourceRecord(MAPPER.writeValueAsString(ddl("app", "ALTER", true, "code"))),
                                collector))
                .hasMessageContaining("Primary keys changed");
        assertThat(output).isEmpty();
    }

    @Test
    void testTableKeyDropAndMissingHistoryFailClosed() throws Exception {
        assertThatThrownBy(() -> parse(guardedParser(), ddl("app", "CREATE", false, "id")))
                .hasMessageContaining("Primary keys changed");
        ObjectNode missing = ddl("app", "ALTER", true, "id");
        ((ObjectNode) missing.path("payload")).remove("historyRecord");
        assertThatThrownBy(() -> parse(guardedParser(), missing))
                .hasMessageContaining("Missing MySQL schema history");
    }

    @Test
    void testUndiscoveredTablesCannotEmitRowsWithoutAValidatedSchema() {
        assertThatThrownBy(() -> parse(guardedParser(), row("other", "id", null, false)))
                .hasMessageContaining("outside startup discovery");
    }

    private static MySqlRecordParser guardedParser() {
        Schema physical = Schema.newBuilder().column("id", DataTypes.INT().notNull()).primaryKey("id").build();
        return parser("a", "UTC", true).withTableSchemaValidator(
                new MySqlTableSchemaValidator(
                        "a", Collections.singletonMap(Identifier.create("app", "orders"), physical), true));
    }

    private static MySqlRecordParser parser(String id, String zone, boolean caseSensitive) {
        Map<String, String> config = new HashMap<>();
        config.put("source." + id + ".hostname", "mysql-" + id);
        config.put("server-time-zone", zone);
        MySqlSourceConfigs sources = MySqlSourceConfigs.fromMap(config);
        CdcMetadataConverter[] converters =
                MySqlMultiSourceSupport.metadataConverters(sources, new CdcMetadataConverter[0]);
        return MySqlMultiSourceSupport.recordParser(
                sources.sources().get(0),
                Collections.emptyList(),
                TypeMapping.defaultMapping(),
                converters,
                caseSensitive);
    }

    private static List<RichCdcMultiplexRecord> parse(MySqlRecordParser parser, ObjectNode event)
            throws Exception {
        List<RichCdcMultiplexRecord> records = new ArrayList<>();
        parser.flatMap(
                new CdcSourceRecord(MAPPER.writeValueAsString(event)),
                new Collector<RichCdcMultiplexRecord>() {
                    @Override
                    public void collect(RichCdcMultiplexRecord record) {
                        records.add(record);
                    }

                    @Override
                    public void close() {}
                });
        return records;
    }

    private static ObjectNode row(String db, String fieldName, String logicalType, boolean update) {
        ObjectNode event = MAPPER.createObjectNode();
        ArrayNode fields = event.putObject("schema").putArray("fields");
        ObjectNode field =
                fields.addObject()
                        .put("field", "after")
                        .put("type", "struct")
                        .putArray("fields")
                        .addObject()
                        .put("field", fieldName)
                        .put("type", logicalType == null ? "int32" : "string");
        if (logicalType != null) {
            field.put("name", logicalType);
        }
        ObjectNode payload = event.putObject("payload");
        payload.put("op", update ? "u" : "c");
        payload.putObject("source").put("db", db).put("table", "orders");
        payload.putObject("after").put(fieldName, 1);
        if (update) {
            payload.putObject("before").put(fieldName, 1);
        }
        return event;
    }

    private static ObjectNode ddl(String db, String type, boolean primaryKey, String fieldName)
            throws Exception {
        ObjectNode history = MAPPER.createObjectNode();
        history.put("databaseName", db).put("ddl", "test DDL");
        ObjectNode table =
                history.putArray("tableChanges")
                        .addObject()
                        .put("type", type)
                        .put("id", db + ".orders")
                        .putObject("table");
        table.put("defaultCharsetName", "utf8mb4");
        ArrayNode keys = table.putArray("primaryKeyColumnNames");
        if (primaryKey) {
            keys.add(fieldName);
        }
        table.putArray("columns")
                .addObject()
                .put("name", fieldName)
                .put("jdbcType", 4)
                .put("typeName", "INT")
                .put("typeExpression", "INT")
                .put("length", 11)
                .put("position", 1)
                .put("optional", false)
                .put("autoIncremented", false)
                .put("generated", false);
        ObjectNode event = MAPPER.createObjectNode();
        ObjectNode payload = event.putObject("payload");
        payload.putObject("source").put("db", db).put("table", "orders");
        payload.put("historyRecord", MAPPER.writeValueAsString(history));
        return event;
    }
}
