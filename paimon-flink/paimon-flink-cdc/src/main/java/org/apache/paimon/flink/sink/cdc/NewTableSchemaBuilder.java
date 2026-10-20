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

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.schema.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.buildPaimonSchema;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.listCaseConvert;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Build schema for new table found in database synchronization. */
public class NewTableSchemaBuilder implements Serializable {

    private final Map<String, String> tableConfig;
    private final boolean caseSensitive;
    private final List<String> partitionKeys;
    private final List<String> primaryKeys;
    private final boolean requirePrimaryKeys;
    private final boolean syncPKeysFromSourceSchema;
    private final CdcMetadataConverter[] metadataConverters;
    private final Map<String, List<String>> partitionKeyMultiple;
    private List<String> primaryKeyPrefix = Collections.emptyList();
    private boolean strictlyCheckSpecified = false;
    private boolean validateTableSchema = false;

    public NewTableSchemaBuilder(
            Map<String, String> tableConfig,
            boolean caseSensitive,
            List<String> partitionKeys,
            List<String> primaryKeys,
            boolean requirePrimaryKeys,
            boolean syncPKeysFromSourceSchema,
            Map<String, List<String>> partitionKeyMultiple,
            CdcMetadataConverter[] metadataConverters) {
        this.tableConfig = tableConfig;
        this.caseSensitive = caseSensitive;
        this.metadataConverters = metadataConverters;
        this.partitionKeys = partitionKeys;
        this.primaryKeys = primaryKeys;
        this.requirePrimaryKeys = requirePrimaryKeys;
        this.syncPKeysFromSourceSchema = syncPKeysFromSourceSchema;
        this.partitionKeyMultiple = partitionKeyMultiple;
    }

    /** Prepends source identity columns to each table's resolved business primary key. */
    public NewTableSchemaBuilder withPrimaryKeyPrefix(List<String> primaryKeyPrefix) {
        this.primaryKeyPrefix = new ArrayList<>(primaryKeyPrefix);
        return this;
    }

    /** Requires explicitly configured keys to exist in newly discovered tables. */
    public NewTableSchemaBuilder withStrictlyCheckSpecified(boolean strictlyCheckSpecified) {
        this.strictlyCheckSpecified = strictlyCheckSpecified;
        return this;
    }

    /** Also validates physical shards mapped to a table that already exists. */
    public NewTableSchemaBuilder withTableSchemaValidation(boolean validateTableSchema) {
        this.validateTableSchema = validateTableSchema;
        return this;
    }

    /** Resolves the same key policy as table creation without consulting the created-table cache. */
    public Optional<Schema> buildForValidation(RichCdcMultiplexRecord record) {
        if (!validateTableSchema) {
            return Optional.empty();
        }
        Optional<Schema> expected = build(record);
        expected.ifPresent(
                schema -> {
                    List<String> sourceKeys =
                            listCaseConvert(record.cdcSchema().primaryKeys(), caseSensitive);
                    checkArgument(
                            !sourceKeys.isEmpty() && schema.primaryKeys().containsAll(sourceKeys),
                            "Physical source table '%s.%s' has primary keys %s, but the resolved "
                                    + "target primary keys %s do not retain them. Explicit primary "
                                    + "keys must not discard a physical shard's uniqueness.",
                            record.databaseName(),
                            record.tableName(),
                            sourceKeys,
                            schema.primaryKeys());
                });
        return expected;
    }

    public Optional<Schema> build(RichCdcMultiplexRecord record) {
        Schema sourceSchema = record.buildSchema();
        List<String> specifiedPartitionKeys = new ArrayList<>();

        List<String> partitionKeyMultipleList = partitionKeyMultiple.get(record.tableName());
        if (partitionKeyMultipleList != null && !partitionKeyMultipleList.isEmpty()) {
            specifiedPartitionKeys = partitionKeyMultipleList;
        } else if (partitionKeys != null && !partitionKeys.isEmpty()) {
            specifiedPartitionKeys = partitionKeys;
        }

        Schema schema =
                buildPaimonSchema(
                        record.tableName(),
                        specifiedPartitionKeys,
                        primaryKeys,
                        Collections.emptyList(),
                        tableConfig,
                        sourceSchema,
                        metadataConverters,
                        caseSensitive,
                        strictlyCheckSpecified,
                        requirePrimaryKeys,
                        syncPKeysFromSourceSchema);
        if (!primaryKeyPrefix.isEmpty()) {
            checkArgument(
                    schema.primaryKeys().stream().anyMatch(k -> !primaryKeyPrefix.contains(k)),
                    "Source identity columns must accompany a business primary key.");
            LinkedHashSet<String> keys = new LinkedHashSet<>(primaryKeyPrefix);
            keys.addAll(schema.primaryKeys());
            schema =
                    new Schema(
                            schema.fields(),
                            schema.partitionKeys(),
                            new ArrayList<>(keys),
                            schema.options(),
                            schema.comment());
        }
        return Optional.of(schema);
    }
}
