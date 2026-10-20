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
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.SerializablePredicate;
import org.apache.paimon.utils.TypeUtils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.assertSchemaCompatible;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** Shared schema and parsing rules for named MySQL sources. */
final class MySqlMultiSourceSupport {

    static final List<String> SOURCE_IDENTITY_COLUMNS =
            Collections.unmodifiableList(Arrays.asList("source_id", "database_name", "table_name"));

    private MySqlMultiSourceSupport() {}

    static CdcMetadataConverter[] metadataConverters(
            MySqlSourceConfigs sources, CdcMetadataConverter[] configured) {
        if (!sources.isolatePrimaryKeys()) {
            return configured;
        }
        List<CdcMetadataConverter> converters = new ArrayList<>(Arrays.asList(configured));
        Set<String> names = new LinkedHashSet<>();
        for (CdcMetadataConverter converter : configured) {
            checkArgument(
                    names.add(converter.columnName()),
                    "Duplicate metadata column '%s'.",
                    converter.columnName());
        }
        CdcMetadataConverter[] identityConverters = {
            new CdcMetadataConverter.SourceIdConverter("default"),
            new CdcMetadataConverter.DatabaseNameConverter(),
            new CdcMetadataConverter.TableNameConverter()
        };
        for (CdcMetadataConverter converter : identityConverters) {
            if (names.add(converter.columnName())) {
                converters.add(converter);
            }
        }
        return converters.toArray(new CdcMetadataConverter[0]);
    }

    static MySqlRecordParser recordParser(
            MySqlSourceConfigs.SourceConfig source,
            List<ComputedColumn> computedColumns,
            TypeMapping typeMapping,
            CdcMetadataConverter[] metadataConverters,
            boolean caseSensitive) {
        CdcMetadataConverter[] bound =
                Arrays.stream(metadataConverters)
                        .map(
                                converter ->
                                        converter instanceof CdcMetadataConverter.SourceIdConverter
                                                ? new CdcMetadataConverter.SourceIdConverter(
                                                        source.id())
                                                : converter)
                        .toArray(CdcMetadataConverter[]::new);
        return new MySqlRecordParser(
                source.config(), computedColumns, typeMapping, bound, caseSensitive);
    }

    static Schema applyPrimaryKeyMode(Schema schema, boolean isolated) {
        if (!isolated) {
            return schema;
        }
        checkArgument(
                schema.primaryKeys().stream().anyMatch(k -> !SOURCE_IDENTITY_COLUMNS.contains(k)),
                "Source identity columns must accompany a business primary key.");
        LinkedHashSet<String> keys = new LinkedHashSet<>(SOURCE_IDENTITY_COLUMNS);
        keys.addAll(schema.primaryKeys());
        return new Schema(
                schema.fields(),
                schema.partitionKeys(),
                new ArrayList<>(keys),
                schema.options(),
                schema.comment());
    }

    static void validateExistingTable(TableSchema existing, Schema expected, String location) {
        checkArgument(
                existing.primaryKeys().equals(expected.primaryKeys()),
                "Primary keys of existing Paimon table '%s' are %s, but named MySQL sources require %s. "
                        + "Use a new target table or the matching multi-source.key-mode; primary keys cannot be changed in place.",
                location,
                existing.primaryKeys(),
                expected.primaryKeys());
        checkArgument(
                existing.partitionKeys().equals(expected.partitionKeys()),
                "Partition keys of existing Paimon table '%s' are %s, but the job requires %s.",
                location,
                existing.partitionKeys(),
                expected.partitionKeys());
        assertSchemaCompatible(existing, expected.fields());
    }

    static NewTableSchemaBuilder withPrimaryKeyMode(
            NewTableSchemaBuilder builder, boolean isolated) {
        builder.withStrictlyCheckSpecified(true).withTableSchemaValidation(true);
        return isolated ? builder.withPrimaryKeyPrefix(SOURCE_IDENTITY_COLUMNS) : builder;
    }

    /** Keeps startup and runtime exclusions ahead of metadata validation and typed key routing. */
    static SerializablePredicate<Identifier> tableFilter(
            String includingDbs,
            @Nullable String excludingDbs,
            String includingTables,
            @Nullable String excludingTables,
            List<Identifier> sourceExcludedTables,
            Set<String> rejectedTargetTables,
            TableNameConverter tableNameConverter) {
        return new SourceTableFilter(
                includingDbs,
                excludingDbs,
                includingTables,
                excludingTables,
                sourceExcludedTables,
                rejectedTargetTables,
                tableNameConverter);
    }

    private static final class SourceTableFilter implements SerializablePredicate<Identifier> {
        private static final long serialVersionUID = 1L;

        private final Pattern includingDbs;
        @Nullable private final Pattern excludingDbs;
        private final Pattern includingTables;
        @Nullable private final Pattern excludingTables;
        private final Set<Identifier> sourceExcludedTables;
        private final Set<String> rejectedTargetTables;
        private final TableNameConverter tableNameConverter;

        private SourceTableFilter(
                String includingDbs,
                @Nullable String excludingDbs,
                String includingTables,
                @Nullable String excludingTables,
                List<Identifier> sourceExcludedTables,
                Set<String> rejectedTargetTables,
                TableNameConverter tableNameConverter) {
            this.includingDbs = Pattern.compile(includingDbs);
            this.excludingDbs = excludingDbs == null ? null : Pattern.compile(excludingDbs);
            this.includingTables = Pattern.compile(includingTables);
            this.excludingTables =
                    excludingTables == null ? null : Pattern.compile(excludingTables);
            this.sourceExcludedTables = new HashSet<>(sourceExcludedTables);
            this.rejectedTargetTables = new HashSet<>(rejectedTargetTables);
            this.tableNameConverter = tableNameConverter;
        }

        @Override
        public boolean test(Identifier sourceTable) {
            String database = sourceTable.getDatabaseName();
            String table = sourceTable.getObjectName();
            return includingDbs.matcher(database).matches()
                    && (excludingDbs == null || !excludingDbs.matcher(database).matches())
                    && includingTables.matcher(table).matches()
                    && (excludingTables == null || !excludingTables.matcher(table).matches())
                    && !sourceExcludedTables.contains(sourceTable)
                    && !rejectedTargetTables.contains(tableNameConverter.convert(sourceTable));
        }
    }

    static DataStream<RichCdcMultiplexRecord> routeTableRecords(
            DataStream<RichCdcMultiplexRecord> input,
            String target,
            List<DataField> primaryKeys,
            boolean caseSensitive,
            int parallelism) {
        return routeRecords(
                input,
                new RecordKeySelector(
                        target, null, Collections.singletonMap(target, primaryKeys), caseSensitive),
                parallelism);
    }

    static DataStream<RichCdcMultiplexRecord> routeDatabaseRecords(
            DataStream<RichCdcMultiplexRecord> input,
            TableNameConverter tableNameConverter,
            Map<String, List<DataField>> primaryKeys,
            boolean caseSensitive,
            int parallelism) {
        return routeRecords(
                input,
                new RecordKeySelector(null, tableNameConverter, primaryKeys, caseSensitive),
                parallelism);
    }

    private static DataStream<RichCdcMultiplexRecord> routeRecords(
            DataStream<RichCdcMultiplexRecord> input, RecordKeySelector selector, int parallelism) {
        // Rebalancing between a source and its parser can reorder successive changes to the
        // same primary key. Parse at the source parallelism, then route by the actual sink key.
        // The common parallelism also makes the existing CDC sink's forward edges valid.
        return input.keyBy(selector)
                .map((MapFunction<RichCdcMultiplexRecord, RichCdcMultiplexRecord>) record -> record)
                .returns(RichCdcMultiplexRecord.class)
                .name("Route MySQL records by primary key")
                .uid("mysql-multi-source-route")
                .setParallelism(Math.max(1, parallelism));
    }

    /** Hashes are only used for partition routing; this operator does not store keyed state. */
    static final class RecordKeySelector implements KeySelector<RichCdcMultiplexRecord, Integer> {
        private static final long serialVersionUID = 1L;

        @Nullable private final String fixedTarget;
        @Nullable private final TableNameConverter tableNameConverter;
        private final Map<String, List<DataField>> primaryKeys;
        private final boolean caseSensitive;

        RecordKeySelector(
                @Nullable String fixedTarget,
                @Nullable TableNameConverter tableNameConverter,
                Map<String, List<DataField>> primaryKeys,
                boolean caseSensitive) {
            this.fixedTarget = fixedTarget;
            this.tableNameConverter = tableNameConverter;
            this.primaryKeys = new HashMap<>();
            primaryKeys.forEach(
                    (table, keys) -> this.primaryKeys.put(table, new ArrayList<>(keys)));
            this.caseSensitive = caseSensitive;
        }

        @Override
        public Integer getKey(RichCdcMultiplexRecord record) {
            String target =
                    fixedTarget != null
                            ? fixedTarget
                            : tableNameConverter.convert(
                                    Identifier.create(record.databaseName(), record.tableName()));
            int hash = target.hashCode();
            CdcRecord cdcRecord = record.toRichCdcRecord().toCdcRecord();
            Map<String, String> row =
                    caseSensitive ? cdcRecord.data() : cdcRecord.fieldNameLowerCase().data();
            List<DataField> keys = primaryKeys.get(target);
            // A table first discovered at runtime has no startup key definition. Route all its
            // records to one partition instead of guessing keys or changing routing mid-stream.
            // A later restart includes this table in discovery and enables primary-key routing.
            if (keys != null && !row.isEmpty()) {
                Object[] values = new Object[keys.size()];
                for (int i = 0; i < keys.size(); i++) {
                    DataField key = keys.get(i);
                    String value = row.get(key.name());
                    values[i] =
                            value == null
                                    ? null
                                    : TypeUtils.castFromCdcValueString(value, key.type());
                }
                // Use the same value conversion as the sink: strings such as timestamps with
                // different fractional precision can represent the same target primary key.
                // Deep hashing compares binary primary keys by content, not array identity.
                hash = 31 * hash + Arrays.deepHashCode(values);
            }
            return hash;
        }
    }
}
