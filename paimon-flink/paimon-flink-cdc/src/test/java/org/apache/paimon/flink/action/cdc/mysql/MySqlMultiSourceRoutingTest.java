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

import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.mysql.MySqlMultiSourceSupport.RecordKeySelector;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests routing by the final target table and typed Paimon primary key. */
class MySqlMultiSourceRoutingTest {

    @Test
    void testPayloadRowKindAndPhysicalTableDoNotChangeGlobalKeyRoute() {
        RecordKeySelector selector = tableSelector(new DataField(0, "id", DataTypes.INT()));
        RichCdcMultiplexRecord before =
                record(
                        "app_a",
                        "orders_0",
                        RowKind.DELETE,
                        "id",
                        "1",
                        "value",
                        "old",
                        "source_id",
                        "a");
        RichCdcMultiplexRecord after =
                record(
                        "app_b",
                        "orders_1",
                        RowKind.INSERT,
                        "id",
                        "1",
                        "value",
                        "new",
                        "source_id",
                        "b");

        assertThat(selector.getKey(before)).isEqualTo(selector.getKey(after));
        assertThat(selector.getKey(record("orders_0", "id", "2")))
                .isNotEqualTo(selector.getKey(before));
    }

    @Test
    void testDatabaseRoutesByFinalMappedTableName() {
        Map<String, String> mapping = new HashMap<>();
        mapping.put("orders_0", "ods_orders_cdc");
        mapping.put("orders_1", "ods_orders_cdc");
        TableNameConverter converter = new TableNameConverter(true, true, "ods_", "_cdc", mapping);
        Map<String, List<DataField>> keys = new HashMap<>();
        List<DataField> id = Collections.singletonList(new DataField(0, "id", DataTypes.INT()));
        keys.put("ods_orders_cdc", id);
        keys.put("ods_customers_cdc", id);
        RecordKeySelector selector = new RecordKeySelector(null, converter, keys, true);

        Integer expected = selector.getKey(record("orders", "id", "7"));
        assertThat(selector.getKey(record("orders_0", "id", "7"))).isEqualTo(expected);
        assertThat(selector.getKey(record("other_db", "orders_1", RowKind.DELETE, "id", "7")))
                .isEqualTo(expected);
        assertThat(selector.getKey(record("customers", "id", "7"))).isNotEqualTo(expected);
        assertThat(selector.getKey(record("orders_0", "id", "8"))).isNotEqualTo(expected);
    }

    @Test
    void testAllSourceIdentityColumnsParticipateInIsolatedKeyRoute() {
        RecordKeySelector selector =
                tableSelector(
                        new DataField(0, "source_id", DataTypes.STRING()),
                        new DataField(1, "database_name", DataTypes.STRING()),
                        new DataField(2, "table_name", DataTypes.STRING()),
                        new DataField(3, "id", DataTypes.INT()));
        RichCdcMultiplexRecord first =
                record(
                        "orders",
                        "source_id",
                        "a",
                        "database_name",
                        "app",
                        "table_name",
                        "orders",
                        "id",
                        "1");
        Integer expected = selector.getKey(first);

        assertThat(
                        selector.getKey(
                                record(
                                        "orders",
                                        "source_id",
                                        "b",
                                        "database_name",
                                        "app",
                                        "table_name",
                                        "orders",
                                        "id",
                                        "1")))
                .isNotEqualTo(expected);
        assertThat(
                        selector.getKey(
                                record(
                                        "orders",
                                        "source_id",
                                        "a",
                                        "database_name",
                                        "app_2",
                                        "table_name",
                                        "orders",
                                        "id",
                                        "1")))
                .isNotEqualTo(expected);
        assertThat(
                        selector.getKey(
                                record(
                                        "orders",
                                        "source_id",
                                        "a",
                                        "database_name",
                                        "app",
                                        "table_name",
                                        "orders_1",
                                        "id",
                                        "1")))
                .isNotEqualTo(expected);
    }

    @Test
    void testCaseInsensitiveTableAndFieldNames() {
        RecordKeySelector selector =
                new RecordKeySelector(
                        null,
                        new TableNameConverter(false),
                        Collections.singletonMap(
                                "orders",
                                Collections.singletonList(new DataField(0, "id", DataTypes.INT()))),
                        false);

        assertThat(selector.getKey(record("ORDERS", "ID", "42", "VALUE", "before")))
                .isEqualTo(selector.getKey(record("orders", "id", "42", "value", "after")));
        assertThat(selector.getKey(record("ORDERS", "ID", "43")))
                .isNotEqualTo(selector.getKey(record("orders", "id", "42")));
    }

    @Test
    void testRuntimeTablesKeepWholeTableRouting() {
        RecordKeySelector selector =
                new RecordKeySelector(
                        null,
                        new TableNameConverter(true),
                        Collections.singletonMap(
                                "orders",
                                Collections.singletonList(new DataField(0, "id", DataTypes.INT()))),
                        true);
        Integer expected = selector.getKey(record("new_orders", "id", "1", "value", "before"));

        assertThat(selector.getKey(record("new_orders", "id", "2", "value", "after")))
                .isEqualTo(expected);
        assertThat(selector.getKey(record("new_orders"))).isEqualTo(expected);
        assertThat(selector.getKey(record("another_new_table", "id", "1"))).isNotEqualTo(expected);
    }

    @Test
    void testSchemaEventsDoNotRequirePrimaryKeyValues() {
        RecordKeySelector selector = tableSelector(new DataField(0, "ts", DataTypes.TIMESTAMP(6)));

        assertThat(selector.getKey(record("orders")))
                .isEqualTo(selector.getKey(record("orders_1")));
    }

    @Test
    void testTimestampRepresentationsUseTargetPrecision() {
        RecordKeySelector selector = tableSelector(new DataField(0, "ts", DataTypes.TIMESTAMP(6)));
        Integer expected = selector.getKey(record("orders", "ts", "2025-05-06 07:08:09.123"));

        assertThat(selector.getKey(record("orders", "ts", "2025-05-06 07:08:09.123000")))
                .isEqualTo(expected);
        assertThat(selector.getKey(record("orders", "ts", "2025-05-06 07:08:09.123001")))
                .isNotEqualTo(expected);
    }

    @Test
    void testNumericRepresentationsUseTargetTypes() {
        RecordKeySelector selector =
                tableSelector(
                        new DataField(0, "id", DataTypes.INT()),
                        new DataField(1, "amount", DataTypes.DECIMAL(10, 3)));

        assertThat(selector.getKey(record("orders", "id", "01", "amount", "1.2")))
                .isEqualTo(selector.getKey(record("orders", "id", "1", "amount", "1.200")));
    }

    @Test
    void testBigintTargetAcceptsValuesBeyondIntRange() {
        RecordKeySelector selector = tableSelector(new DataField(0, "id", DataTypes.BIGINT()));

        assertThat(selector.getKey(record("orders", "id", "2147483648")))
                .isEqualTo(selector.getKey(record("orders", "id", "02147483648")));
        assertThat(selector.getKey(record("orders", "id", "1")))
                .isEqualTo(selector.getKey(record("orders", "id", "01")));
    }

    @Test
    void testIntTargetRejectsValuesBeyondItsRange() {
        RecordKeySelector selector = tableSelector(new DataField(0, "id", DataTypes.INT()));

        // Target primary-key types are immutable; routing retains the sink's type constraints.
        assertThatThrownBy(() -> selector.getKey(record("orders", "id", "2147483648")))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testNonPrimaryKeyTypeWideningDoesNotChangeRouting() {
        RecordKeySelector selector = tableSelector(new DataField(0, "id", DataTypes.INT()));
        CdcSchema beforeSchema =
                CdcSchema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.INT())
                        .primaryKey(Collections.singletonList("id"))
                        .build();
        CdcSchema afterSchema =
                CdcSchema.newBuilder()
                        .column("id", DataTypes.INT())
                        .column("value", DataTypes.BIGINT())
                        .primaryKey(Collections.singletonList("id"))
                        .build();
        RichCdcMultiplexRecord before =
                new RichCdcMultiplexRecord(
                        "app",
                        "orders",
                        beforeSchema,
                        record("orders", "id", "1", "value", "2147483647")
                                .toRichCdcRecord()
                                .toCdcRecord());
        RichCdcMultiplexRecord after =
                new RichCdcMultiplexRecord(
                        "app",
                        "orders",
                        afterSchema,
                        record("orders", "id", "1", "value", "2147483648")
                                .toRichCdcRecord()
                                .toCdcRecord());

        assertThat(selector.getKey(after)).isEqualTo(selector.getKey(before));
        assertThat(
                        selector.getKey(
                                new RichCdcMultiplexRecord(
                                        "app", "orders", afterSchema, CdcRecord.emptyRecord())))
                .isEqualTo(
                        selector.getKey(
                                new RichCdcMultiplexRecord(
                                        "app", "orders", beforeSchema, CdcRecord.emptyRecord())));
    }

    @Test
    void testBinaryKeysRouteByDecodedContent() {
        RecordKeySelector selector = tableSelector(new DataField(0, "id", DataTypes.BINARY(2)));
        RichCdcMultiplexRecord record = record("orders", "id", "AQI=");
        Integer expected = selector.getKey(record);

        // Each conversion allocates a new byte array; optional Base64 padding changes no bytes.
        assertThat(selector.getKey(record)).isEqualTo(expected);
        assertThat(selector.getKey(record("orders", "id", "AQI"))).isEqualTo(expected);
        assertThat(selector.getKey(record("orders", "id", "AQM="))).isNotEqualTo(expected);
    }

    @Test
    void testVarbinaryKeysRouteByContent() {
        RecordKeySelector selector = tableSelector(new DataField(0, "id", DataTypes.VARBINARY(8)));
        RichCdcMultiplexRecord record = record("orders", "id", "abc");
        Integer expected = selector.getKey(record);

        assertThat(selector.getKey(record)).isEqualTo(expected);
        assertThat(selector.getKey(record("orders", "id", "abd"))).isNotEqualTo(expected);
    }

    @Test
    void testTypedRoutingSurvivesSerialization() throws Exception {
        RecordKeySelector selector =
                new RecordKeySelector(
                        null,
                        new TableNameConverter(
                                true, true, "", "", Collections.singletonMap("orders_0", "orders")),
                        Collections.singletonMap(
                                "orders",
                                Arrays.asList(
                                        new DataField(0, "source_id", DataTypes.STRING()),
                                        new DataField(1, "id", DataTypes.BINARY(2)),
                                        new DataField(2, "ts", DataTypes.TIMESTAMP(6)))),
                        true);
        RecordKeySelector restored = InstantiationUtil.clone(selector);
        RichCdcMultiplexRecord record =
                record(
                        "orders_0",
                        "source_id",
                        "a",
                        "id",
                        "AQI=",
                        "ts",
                        "2025-05-06 07:08:09.123000");

        assertThat(restored.getKey(record)).isEqualTo(selector.getKey(record));
    }

    private static RecordKeySelector tableSelector(DataField... keys) {
        return new RecordKeySelector(
                "target", null, Collections.singletonMap("target", Arrays.asList(keys)), true);
    }

    private static RichCdcMultiplexRecord record(String table, String... fields) {
        return record("app", table, RowKind.INSERT, fields);
    }

    private static RichCdcMultiplexRecord record(
            String database, String table, RowKind kind, String... fields) {
        Map<String, String> row = new HashMap<>();
        for (int i = 0; i < fields.length; i += 2) {
            row.put(fields[i], fields[i + 1]);
        }
        return new RichCdcMultiplexRecord(database, table, null, new CdcRecord(kind, row));
    }
}
