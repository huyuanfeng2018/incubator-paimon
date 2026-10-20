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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Checks the fixed physical shards of a named table-sync source before emitting CDC events. */
final class MySqlTableSchemaValidator implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String sourceId;
    private final Map<Identifier, KeySchema> discoveredSchemas;
    private final boolean caseSensitive;

    MySqlTableSchemaValidator(
            String sourceId, Map<Identifier, Schema> discoveredSchemas, boolean caseSensitive) {
        checkArgument(!discoveredSchemas.isEmpty(), "No physical tables for source '%s'.", sourceId);
        this.sourceId = sourceId;
        this.caseSensitive = caseSensitive;
        this.discoveredSchemas = new LinkedHashMap<>();
        discoveredSchemas.forEach(
                (table, schema) ->
                        this.discoveredSchemas.put(
                                table,
                                new KeySchema(
                                        normalize(schema.primaryKeys(), caseSensitive),
                                        fieldTypes(schema.fields()))));
    }

    /** Literal, deterministic source filters; runtime table discovery is not part of this action. */
    String tableList() {
        return discoveredSchemas.keySet().stream()
                .map(
                        id ->
                                Pattern.quote(id.getDatabaseName())
                                        + "\\."
                                        + Pattern.quote(id.getObjectName()))
                .sorted()
                .collect(Collectors.joining("|"));
    }

    void validateTable(Identifier table) {
        checkArgument(
                discoveredSchemas.containsKey(table),
                "MySQL source '%s' emitted table '%s' outside startup discovery. "
                        + "Restart the table-sync job to discover and validate additional shards.",
                sourceId,
                table.getFullName());
    }

    void validateSchema(Identifier table, CdcSchema actual) {
        validateTable(table);
        KeySchema expected = discoveredSchemas.get(table);
        List<String> expectedKeys = expected.keys;
        List<String> actualKeys = normalize(actual.primaryKeys(), caseSensitive);
        checkArgument(
                !expectedKeys.isEmpty() && expectedKeys.equals(actualKeys),
                "Primary keys changed for MySQL source '%s', table '%s': expected %s but found %s. "
                        + "Named table synchronization does not support primary-key evolution.",
                sourceId,
                table.getFullName(),
                expectedKeys,
                actualKeys);
        Map<String, DataType> expectedTypes = expected.types;
        Map<String, DataType> actualTypes = fieldTypes(actual.fields());
        for (String key : expectedKeys) {
            checkArgument(
                    expectedTypes.get(key) != null
                            && expectedTypes.get(key).equals(actualTypes.get(key)),
                    "Primary-key type changed for MySQL source '%s', table '%s', column '%s': "
                            + "expected %s but found %s. Use an explicit migration.",
                    sourceId,
                    table.getFullName(),
                    key,
                    expectedTypes.get(key),
                    actualTypes.get(key));
        }
    }

    /** Run before any target creation or option mutation, including explicit primary-key mode. */
    static void validateTargetKeys(Schema physical, Schema target, boolean caseSensitive) {
        List<String> physicalKeys = normalize(physical.primaryKeys(), caseSensitive);
        List<String> targetKeys = normalize(target.primaryKeys(), caseSensitive);
        checkArgument(
                !physicalKeys.isEmpty() && targetKeys.containsAll(physicalKeys),
                "The target primary keys %s must retain every physical MySQL primary-key column %s. "
                        + "Explicit primary keys must not discard the source uniqueness constraint.",
                targetKeys,
                physicalKeys);
    }

    /** Schema itself is not serializable; retain only the immutable key contract in the job graph. */
    private static final class KeySchema implements Serializable {
        private static final long serialVersionUID = 1L;
        private final List<String> keys;
        private final Map<String, DataType> types;

        private KeySchema(List<String> keys, Map<String, DataType> allTypes) {
            this.keys = new ArrayList<>(keys);
            this.types = new LinkedHashMap<>();
            for (String key : keys) {
                this.types.put(key, allTypes.get(key));
            }
        }
    }

    private Map<String, DataType> fieldTypes(List<DataField> fields) {
        Map<String, DataType> result = new LinkedHashMap<>();
        for (DataField field : fields) {
            result.put(normalize(field.name(), caseSensitive), field.type().copy(true));
        }
        return result;
    }

    private static List<String> normalize(List<String> names, boolean caseSensitive) {
        List<String> result = new ArrayList<>();
        for (String name : names) {
            result.add(normalize(name, caseSensitive));
        }
        return result;
    }

    private static String normalize(String name, boolean caseSensitive) {
        return caseSensitive ? name : name.toLowerCase(Locale.ROOT);
    }
}
