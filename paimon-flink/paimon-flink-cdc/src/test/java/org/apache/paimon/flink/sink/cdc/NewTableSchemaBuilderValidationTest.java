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

package org.apache.paimon.flink.sink.cdc;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests key validation independently of field evolution and catalog implementation. */
class NewTableSchemaBuilderValidationTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMatchingKeys(boolean isolated) {
        Schema schema = schema(isolated, "id");
        assertThatCode(() -> validate(schema, schema, "id")).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testDifferentPhysicalKey(boolean isolated) {
        assertThatThrownBy(() -> validate(schema(isolated, "id"), schema(isolated, "code"), "code"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Primary keys")
                .hasMessageContaining("app.orders_1 -> target.orders");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testCompositeKeyMismatch(boolean isolated) {
        assertThatThrownBy(
                        () ->
                                validate(
                                        schema(isolated, "id"),
                                        schema(isolated, "id", "code"),
                                        "id",
                                        "code"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Primary keys");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExplicitOverrideCannotHidePhysicalKey(boolean isolated) {
        Schema schema = schema(isolated, "id");
        assertThatThrownBy(() -> validate(schema, schema, "code"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Business primary keys");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testExplicitOverrideCannotDropPartOfCompositeKey(boolean isolated) {
        Schema schema = schema(isolated, "id");
        assertThatThrownBy(() -> validate(schema, schema, "id", "code"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Business primary keys");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testMissingPhysicalKey(boolean isolated) {
        Schema schema = schema(isolated, "id");
        assertThatThrownBy(() -> validate(schema, schema))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Business primary keys");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSafeExplicitSuperset(boolean isolated) {
        Schema schema = schema(isolated, "id", "code");
        assertThatCode(() -> validate(schema, schema, "id")).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testPartitionKeyMismatch(boolean isolated) {
        Schema target = schema(isolated, "id", "code");
        Schema expected =
                new Schema(
                        target.fields(),
                        Collections.singletonList("code"),
                        target.primaryKeys(),
                        target.options(),
                        target.comment());
        assertThatThrownBy(() -> validate(target, expected, "id", "code"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Partition keys");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testOrdinaryFieldEvolutionIsNotRejected(boolean isolated) {
        Schema target = schema(isolated, "id");
        Schema.Builder evolved =
                Schema.newBuilder()
                        .column("id", DataTypes.BIGINT().notNull())
                        .column("code", DataTypes.STRING().notNull())
                        .column("extra", DataTypes.STRING());
        addIdentityColumns(evolved, isolated);
        Schema expected = evolved.primaryKey(target.primaryKeys()).build();
        assertThatCode(() -> validate(target, expected, "id")).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testKeyOrderMustMatch(boolean isolated) {
        assertThatThrownBy(
                        () ->
                                validate(
                                        schema(isolated, "id", "code"),
                                        schema(isolated, "code", "id"),
                                        "code",
                                        "id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Primary keys");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testWrongKeyMode(boolean isolated) {
        assertThatThrownBy(() -> validate(schema(!isolated, "id"), schema(isolated, "id"), "id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Primary keys");
    }

    private static void validate(Schema target, Schema expected, String... sourceKeys) {
        NewTableSchemaBuilder.validateKeys(
                TableSchema.create(0, target),
                expected,
                Arrays.asList(sourceKeys),
                "app.orders_1 -> target.orders");
    }

    private static Schema schema(boolean isolated, String... businessKeys) {
        Schema.Builder builder =
                Schema.newBuilder()
                        .column("id", DataTypes.INT().notNull())
                        .column("code", DataTypes.STRING().notNull());
        List<String> keys = addIdentityColumns(builder, isolated);
        keys.addAll(Arrays.asList(businessKeys));
        return builder.primaryKey(keys).build();
    }

    private static List<String> addIdentityColumns(Schema.Builder builder, boolean isolated) {
        List<String> keys = new ArrayList<>();
        if (isolated) {
            for (String name : Arrays.asList("source_id", "database_name", "table_name")) {
                builder.column(name, DataTypes.STRING().notNull());
                keys.add(name);
            }
        }
        return keys;
    }
}
