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

import org.apache.paimon.flink.action.cdc.CdcMetadataConverter;
import org.apache.paimon.flink.action.cdc.ComputedColumn;
import org.apache.paimon.flink.action.cdc.TypeMapping;
import org.apache.paimon.flink.sink.cdc.CdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.TypeUtils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    static DataStream<RichCdcMultiplexRecord> routeTableRecords(
            DataStream<RichCdcMultiplexRecord> input,
            String target,
            List<DataField> primaryKeys,
            boolean caseSensitive,
            int parallelism) {
        return routeRecords(
                input, new RecordKeySelector(target, primaryKeys, caseSensitive), parallelism);
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

        private final String target;
        private final List<DataField> primaryKeys;
        private final boolean caseSensitive;

        RecordKeySelector(String target, List<DataField> primaryKeys, boolean caseSensitive) {
            this.target = target;
            this.primaryKeys = new ArrayList<>(primaryKeys);
            this.caseSensitive = caseSensitive;
        }

        @Override
        public Integer getKey(RichCdcMultiplexRecord record) {
            int hash = target.hashCode();
            CdcRecord cdcRecord = record.toRichCdcRecord().toCdcRecord();
            Map<String, String> row =
                    caseSensitive ? cdcRecord.data() : cdcRecord.fieldNameLowerCase().data();
            List<DataField> keys = primaryKeys;
            if (!row.isEmpty()) {
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
