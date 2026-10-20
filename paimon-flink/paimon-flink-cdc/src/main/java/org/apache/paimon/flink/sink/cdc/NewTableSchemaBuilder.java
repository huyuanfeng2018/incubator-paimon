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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;

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
    private CatalogLoader validationCatalogLoader;
    private String validationDatabase;

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

    /** Enables validation of every source schema against the actual target, not only new tables. */
    public NewTableSchemaBuilder withTargetSchemaValidation(
            String database, CatalogLoader catalogLoader) {
        this.validationDatabase = database;
        this.validationCatalogLoader = catalogLoader;
        return this;
    }

    /** Whether schema-bearing events require target-key validation. */
    public boolean isTargetSchemaValidationEnabled() {
        return validationCatalogLoader != null;
    }

    /**
     * Validates a physical shard after target creation and before schema or row output. Do not
     * cache validation by target name: another source, or a later schema event, may have different
     * business keys even when it maps to a previously validated target.
     */
    public void validateTargetSchema(RichCdcMultiplexRecord record, String targetTable) {
        if (validationCatalogLoader == null || record.cdcSchema().fields().isEmpty()) {
            return;
        }

        Schema expected = build(record).get();
        Identifier target = Identifier.create(validationDatabase, targetTable);
        String location =
                record.databaseName() + "." + record.tableName() + " -> " + target.getFullName();
        // Catalog work is limited to schema events. A fresh catalog also checks the winner of
        // concurrent createTable(ignoreIfExists=true) calls and requires no restored cache.
        try (Catalog catalog = validationCatalogLoader.load()) {
            TableSchema existing = ((FileStoreTable) catalog.getTable(target)).schema();
            validateKeys(
                    existing,
                    expected,
                    listCaseConvert(record.cdcSchema().primaryKeys(), caseSensitive),
                    location);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Cannot validate keys for source shard " + location, e);
        }
    }

    static void validateKeys(
            TableSchema existing,
            Schema expected,
            List<String> sourcePrimaryKeys,
            String location) {
        // An explicit --primary_keys must not hide an incompatible physical primary key. A
        // target key that omits part of the source key can silently merge distinct source rows.
        checkArgument(
                !sourcePrimaryKeys.isEmpty()
                        && expected.primaryKeys().containsAll(sourcePrimaryKeys),
                "Business primary keys of source shard '%s' are %s, but target keys %s do not "
                        + "preserve them. Refusing to ingest this shard to prevent row overwrites.",
                location,
                sourcePrimaryKeys,
                expected.primaryKeys());
        checkArgument(
                existing.primaryKeys().equals(expected.primaryKeys()),
                "Primary keys of existing Paimon target for source shard '%s' are %s, but the "
                        + "shard requires %s. Refusing to ingest this shard to prevent row overwrites.",
                location,
                existing.primaryKeys(),
                expected.primaryKeys());
        checkArgument(
                existing.partitionKeys().equals(expected.partitionKeys()),
                "Partition keys of existing Paimon target for source shard '%s' are %s, but the "
                        + "shard requires %s.",
                location,
                existing.partitionKeys(),
                expected.partitionKeys());
        // Do not compare all fields here: normal add-column and type-widening events must
        // reach the existing schema-evolution operator.
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
