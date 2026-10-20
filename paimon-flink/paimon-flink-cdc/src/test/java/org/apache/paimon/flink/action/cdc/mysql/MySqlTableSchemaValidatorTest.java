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
import org.apache.paimon.flink.sink.cdc.CdcSchema;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Table-only source constraints: key changes fail before any parsed event can be emitted. */
class MySqlTableSchemaValidatorTest {

    private static final Identifier TABLE = Identifier.create("app", "orders");

    @Test
    void testOrdinaryColumnsMayEvolveWithoutChangingPhysicalKeys() {
        MySqlTableSchemaValidator validator = validator(true);
        assertThatCode(
                        () -> validator.validateSchema(
                                TABLE,
                                CdcSchema.newBuilder()
                                        .column("id", DataTypes.INT())
                                        .column("new_column", DataTypes.STRING())
                                        .primaryKey(Collections.singletonList("id"))
                                        .build()))
                .doesNotThrowAnyException();
    }

    @Test
    void testChangedPhysicalKeyIsRejectedEvenWhenOldKeyIsStillAColumn() {
        CdcSchema changed = CdcSchema.newBuilder()
                .column("id", DataTypes.INT())
                .column("code", DataTypes.INT())
                .primaryKey(Collections.singletonList("code"))
                .build();
        assertThatThrownBy(() -> validator(true).validateSchema(TABLE, changed))
                .hasMessageContaining("Primary keys changed")
                .hasMessageContaining("source 'a'");
    }

    @Test
    void testDroppingPrimaryKeyFailsRatherThanSilentlySuppressingTheTable() {
        assertThatThrownBy(
                        () -> validator(true).validateSchema(
                                TABLE, CdcSchema.newBuilder().column("id", DataTypes.INT()).build()))
                .hasMessageContaining("Primary keys changed");
    }

    @Test
    void testChangingPrimaryKeyTypeRequiresMigration() {
        assertThatThrownBy(
                        () -> validator(true).validateSchema(
                                TABLE,
                                CdcSchema.newBuilder()
                                        .column("id", DataTypes.BIGINT())
                                        .primaryKey(Collections.singletonList("id"))
                                        .build()))
                .hasMessageContaining("Primary-key type changed");
    }

    @Test
    void testUnknownPhysicalTableCannotJoinAFixedTarget() {
        assertThatThrownBy(() -> validator(true).validateTable(Identifier.create("other", "orders")))
                .hasMessageContaining("outside startup discovery");
    }

    @Test
    void testCaseInsensitiveKeyComparison() {
        CdcSchema upper = CdcSchema.newBuilder()
                .column("ID", DataTypes.INT())
                .primaryKey(Collections.singletonList("ID"))
                .build();
        assertThatCode(() -> validator(false).validateSchema(TABLE, upper)).doesNotThrowAnyException();
        assertThatThrownBy(() -> validator(true).validateSchema(TABLE, upper))
                .hasMessageContaining("Primary keys changed");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTargetCannotDiscardPhysicalKeys(boolean isolated) {
        Schema target = target(isolated, "code");
        assertThatThrownBy(() -> MySqlTableSchemaValidator.validateTargetKeys(physical(), target, true))
                .hasMessageContaining("must retain every physical MySQL primary-key column");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testTargetMayRetainPhysicalKeysAsASuperset(boolean isolated) {
        Schema target = target(isolated, "id", "code");
        assertThatCode(() -> MySqlTableSchemaValidator.validateTargetKeys(physical(), target, true))
                .doesNotThrowAnyException();
    }

    @Test
    void testConflictingMergedSourceKeysCannotBeMaskedByExplicitTargetKeys() {
        Schema source = Schema.newBuilder().column("id", DataTypes.INT()).build();
        assertThatThrownBy(
                        () -> MySqlTableSchemaValidator.validateTargetKeys(source, target(false, "id"), true))
                .hasMessageContaining("must retain every physical MySQL primary-key column");
    }

    @Test
    void testFixedTableFilterIsLiteralAndDeterministic() throws Exception {
        Map<Identifier, Schema> discovered = new LinkedHashMap<>();
        discovered.put(Identifier.create("db.a", "orders+1"), physical());
        discovered.put(TABLE, physical());
        MySqlTableSchemaValidator validator = new MySqlTableSchemaValidator("a", discovered, true);
        Pattern filter = Pattern.compile(validator.tableList());
        assertThat(filter.matcher("db.a.orders+1").matches()).isTrue();
        assertThat(filter.matcher("app.orders").matches()).isTrue();
        assertThat(filter.matcher("dbXa.orderss1").matches()).isFalse();
        assertThat(filter.matcher("app.orders_1").matches()).isFalse();
        MySqlTableSchemaValidator restored = InstantiationUtil.clone(validator);
        assertThat(restored.tableList()).isEqualTo(validator.tableList());
        assertThatThrownBy(() -> restored.validateTable(Identifier.create("app", "orders_1")))
                .hasMessageContaining("outside startup discovery");
    }

    private static MySqlTableSchemaValidator validator(boolean caseSensitive) {
        return new MySqlTableSchemaValidator("a", Collections.singletonMap(TABLE, physical()), caseSensitive);
    }

    private static Schema physical() {
        return Schema.newBuilder()
                .column("id", DataTypes.INT().notNull())
                .column("code", DataTypes.INT())
                .primaryKey("id")
                .build();
    }

    private static Schema target(boolean isolated, String... keys) {
        Schema.Builder builder = Schema.newBuilder()
                .column("id", DataTypes.INT())
                .column("code", DataTypes.INT());
        if (isolated) {
            builder.column("source_id", DataTypes.STRING())
                    .column("database_name", DataTypes.STRING())
                    .column("table_name", DataTypes.STRING());
        }
        return MySqlMultiSourceSupport.applyPrimaryKeyMode(builder.primaryKey(Arrays.asList(keys)).build(), isolated);
    }
}
