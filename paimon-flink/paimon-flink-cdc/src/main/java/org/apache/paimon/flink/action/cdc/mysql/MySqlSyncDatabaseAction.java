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

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.CdcActionCommonUtils;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.SyncDatabaseActionBase;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.TableNameConverter;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemaUtils;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractor;
import org.apache.paimon.flink.sink.cdc.NewTableSchemaBuilder;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SerializablePredicate;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.DIVIDED;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.schemaCompatible;
import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.tableList;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize the whole MySQL database into one Paimon database.
 *
 * <p>You should specify MySQL source database in {@code mySqlConfig}. See <a
 * href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>For each MySQL table to be synchronized, if the corresponding Paimon table does not exist,
 * this action will automatically create the table. Its schema will be derived from all specified
 * MySQL tables. If the Paimon table already exists, its schema will be compared against the schema
 * of all specified MySQL tables.
 *
 * <p>This action supports a limited number of schema changes. Currently, the framework can not drop
 * columns, so the behaviors of `DROP` will be ignored, `RENAME` will add a new column. Currently
 * supported schema changes includes:
 *
 * <ul>
 *   <li>Adding columns.
 *   <li>Altering column types. More specifically,
 *       <ul>
 *         <li>altering from a string type (char, varchar, text) to another string type with longer
 *             length,
 *         <li>altering from a binary type (binary, varbinary, blob) to another binary type with
 *             longer length,
 *         <li>altering from an integer type (tinyint, smallint, int, bigint) to another integer
 *             type with wider range,
 *         <li>altering from a floating-point type (float, double) to another floating-point type
 *             with wider range,
 *       </ul>
 *       are supported.
 * </ul>
 *
 * <p>This action creates a Paimon table sink for each Paimon table to be written, so this action is
 * not very efficient in resource saving. We may optimize this action by merging all sinks into one
 * instance in the future.
 */
public class MySqlSyncDatabaseAction extends SyncDatabaseActionBase {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlSyncDatabaseAction.class);

    private boolean ignoreIncompatible = false;

    private final MySqlSourceConfigs sourceConfigs;

    private final Map<String, List<Identifier>> monitoredTablesBySource = new LinkedHashMap<>();
    private final Map<String, List<Identifier>> excludedTablesBySource = new LinkedHashMap<>();
    private final Set<String> rejectedTargetTables = new HashSet<>();

    // for test purpose
    private final List<Identifier> monitoredTables = new ArrayList<>();
    private final List<Identifier> excludedTables = new ArrayList<>();

    public MySqlSyncDatabaseAction(
            String database, Map<String, String> catalogConfig, Map<String, String> mySqlConfig) {
        this(database, catalogConfig, MySqlSourceConfigs.fromMap(mySqlConfig));
    }

    private MySqlSyncDatabaseAction(
            String database, Map<String, String> catalogConfig, MySqlSourceConfigs sourceConfigs) {
        super(
                database,
                catalogConfig,
                sourceConfigs.firstConfig().toMap(),
                SyncJobHandler.SourceType.MYSQL);
        this.sourceConfigs = sourceConfigs;
        this.mode = DIVIDED;
    }

    public MySqlSyncDatabaseAction ignoreIncompatible(boolean ignoreIncompatible) {
        this.ignoreIncompatible = ignoreIncompatible;
        return this;
    }

    @Override
    protected void validateSourceOptions() {
        if (!sourceConfigs.isNamed()) {
            super.validateSourceOptions();
            return;
        }

        checkArgument(mergeShards, "Named MySQL sources require --merge_shards true.");
        checkArgument(
                syncPKeysFromSourceSchema,
                "Named MySQL sources require --sync_primary_keys_from_source_schema true.");
        checkArgument(
                dbPrefix.isEmpty() && dbSuffix.isEmpty(),
                "Named MySQL sources do not support --table_prefix_db or --table_suffix_db. "
                        + "Use --table_prefix, --table_suffix or --table_mapping instead.");
        checkArgument(
                computedColumns.isEmpty(),
                "Named MySQL database synchronization does not support --computed_column.");
        validateRuntimeExecutionMode();
        sourceConfigs.validate(false, env.getParallelism());
        metadataConverters =
                MySqlMultiSourceSupport.metadataConverters(sourceConfigs, metadataConverters);
    }

    @Override
    protected void beforeBuildingSourceSink() throws Exception {
        if (sourceConfigs.isNamed()) {
            beforeBuildingNamedSources();
            return;
        }

        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        JdbcSchemasInfo mySqlSchemasInfo =
                MySqlActionUtils.getMySqlTableInfos(
                        cdcSourceConfig,
                        tableName ->
                                shouldMonitorTable(tableName, includingPattern, excludingPattern),
                        excludedTables,
                        typeMapping);

        logNonPkTables(mySqlSchemasInfo.nonPkTables());
        List<JdbcTableInfo> jdbcTableInfos = mySqlSchemasInfo.toMySqlTableInfos(mergeShards);

        checkArgument(
                !jdbcTableInfos.isEmpty(),
                "No tables found in MySQL database "
                        + cdcSourceConfig.get(MySqlSourceOptions.DATABASE_NAME)
                        + ", or MySQL database does not exist.");

        TableNameConverter tableNameConverter =
                new TableNameConverter(
                        caseSensitive, mergeShards, tablePrefix, tableSuffix, tableMapping);
        for (JdbcTableInfo tableInfo : jdbcTableInfos) {
            Identifier identifier =
                    Identifier.create(
                            database,
                            tableNameConverter.convert("", tableInfo.toPaimonTableName()));
            FileStoreTable table;
            Schema fromMySql =
                    CdcActionCommonUtils.buildPaimonSchema(
                            identifier.getFullName(),
                            partitionKeys,
                            primaryKeys,
                            Collections.emptyList(),
                            tableConfig,
                            tableInfo.schema(),
                            metadataConverters,
                            caseSensitive,
                            false,
                            true,
                            this.syncPKeysFromSourceSchema);
            try {
                table = (FileStoreTable) catalog.getTable(identifier);
                Supplier<String> errMsg =
                        incompatibleMessage(table.schema(), tableInfo, identifier);
                if (shouldMonitorTable(table.schema(), fromMySql, errMsg)) {
                    table = alterTableOptions(identifier, table);
                    tables.add(table);
                    monitoredTables.addAll(tableInfo.identifiers());
                } else {
                    excludedTables.addAll(tableInfo.identifiers());
                }
            } catch (Catalog.TableNotExistException e) {
                catalog.createTable(identifier, fromMySql, false);
                table = (FileStoreTable) catalog.getTable(identifier);
                tables.add(table);
                monitoredTables.addAll(tableInfo.identifiers());
            }
        }

        Preconditions.checkState(
                !monitoredTables.isEmpty(),
                "No tables to be synchronized. Possible cause is the schemas of all tables in specified "
                        + "MySQL database are not compatible with those of existed Paimon tables. Please check the log.");
    }

    private void beforeBuildingNamedSources() throws Exception {
        tables.clear();
        monitoredTables.clear();
        excludedTables.clear();
        monitoredTablesBySource.clear();
        excludedTablesBySource.clear();
        rejectedTargetTables.clear();

        Pattern includingPattern = Pattern.compile(includingTables);
        Pattern excludingPattern =
                excludingTables == null ? null : Pattern.compile(excludingTables);
        Pattern includingDbPattern = Pattern.compile(includingDbs);
        Pattern excludingDbPattern = excludingDbs == null ? null : Pattern.compile(excludingDbs);
        TableNameConverter nameConverter =
                new TableNameConverter(caseSensitive, true, tablePrefix, tableSuffix, tableMapping);

        Map<String, List<JdbcSchemasInfo.JdbcSchemaInfo>> schemasBySource = new LinkedHashMap<>();
        Map<String, Schema> mergedSchemas = new TreeMap<>();
        Map<String, String> sourceLocations = new LinkedHashMap<>();
        Map<String, List<String>> targetPartitionKeys = new LinkedHashMap<>();

        // Discover every instance before changing any target table. Identical database and table
        // names from different instances must retain independent source filters.
        for (MySqlSourceConfigs.SourceConfig source : sourceConfigs.sources()) {
            List<Identifier> sourceExcluded = new ArrayList<>();
            excludedTablesBySource.put(source.id(), sourceExcluded);
            monitoredTablesBySource.put(source.id(), new ArrayList<>());
            JdbcSchemasInfo sourceSchemas;
            try {
                sourceSchemas =
                        discoverSource(
                                source,
                                name ->
                                        shouldMonitorTable(
                                                name, includingPattern, excludingPattern),
                                sourceExcluded);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                        "Cannot discover MySQL source '"
                                + source.id()
                                + "'. Check its connectivity and database/table filters.",
                        e);
            }
            sourceExcluded.addAll(sourceSchemas.nonPkTables());
            List<JdbcSchemasInfo.JdbcSchemaInfo> selected = new ArrayList<>();
            for (JdbcSchemasInfo.JdbcSchemaInfo table : sourceSchemas.pkTables()) {
                String sourceDb = table.identifier().getDatabaseName();
                if (!includingDbPattern.matcher(sourceDb).matches()
                        || (excludingDbPattern != null
                                && excludingDbPattern.matcher(sourceDb).matches())) {
                    sourceExcluded.add(table.identifier());
                } else {
                    selected.add(table);
                }
            }
            selected.sort(Comparator.comparing(table -> table.identifier().getFullName()));
            checkArgument(
                    !selected.isEmpty(),
                    "No tables with primary keys match the filters for MySQL source '%s'.",
                    source.id());
            schemasBySource.put(source.id(), selected);

            for (JdbcSchemasInfo.JdbcSchemaInfo table : selected) {
                String target = nameConverter.convert(table.identifier());
                String location = source.id() + ":" + table.identifier().getFullName();
                List<String> specifiedPartitionKeys =
                        partitionKeyMultiple.get(table.identifier().getObjectName());
                if (specifiedPartitionKeys == null || specifiedPartitionKeys.isEmpty()) {
                    specifiedPartitionKeys = partitionKeys;
                }
                Schema previous = mergedSchemas.get(target);
                if (previous == null) {
                    mergedSchemas.put(target, table.schema());
                    sourceLocations.put(target, location);
                    targetPartitionKeys.put(target, specifiedPartitionKeys);
                } else {
                    checkArgument(
                            targetPartitionKeys.get(target).equals(specifiedPartitionKeys),
                            "Source tables mapped to Paimon table '%s' have different partition keys.",
                            target);
                    mergedSchemas.put(
                            target,
                            JdbcSchemaUtils.mergeSchema(
                                    sourceLocations.get(target),
                                    previous,
                                    location,
                                    table.schema()));
                    sourceLocations.put(target, sourceLocations.get(target) + ", " + location);
                }
            }
        }

        Map<String, Schema> acceptedSchemas = new LinkedHashMap<>();
        Map<String, FileStoreTable> existingTables = new LinkedHashMap<>();
        for (Map.Entry<String, Schema> entry : mergedSchemas.entrySet()) {
            Identifier identifier = Identifier.create(database, entry.getKey());
            Schema fromMySql =
                    MySqlMultiSourceSupport.applyPrimaryKeyMode(
                            CdcActionCommonUtils.buildPaimonSchema(
                                    identifier.getFullName(),
                                    targetPartitionKeys.get(entry.getKey()),
                                    primaryKeys,
                                    Collections.emptyList(),
                                    tableConfig,
                                    entry.getValue(),
                                    metadataConverters,
                                    caseSensitive,
                                    true,
                                    true,
                                    true),
                            sourceConfigs.isolatePrimaryKeys());
            try {
                FileStoreTable existing = (FileStoreTable) catalog.getTable(identifier);
                try {
                    MySqlMultiSourceSupport.validateExistingTable(
                            existing.schema(),
                            fromMySql,
                            identifier.getFullName()
                                    + " from "
                                    + sourceLocations.get(entry.getKey()));
                } catch (IllegalArgumentException e) {
                    if (!ignoreIncompatible) {
                        throw e;
                    }
                    LOG.warn(
                            "All shards mapped to Paimon table '{}' will be excluded: {}",
                            identifier.getFullName(),
                            e.getMessage());
                    rejectedTargetTables.add(entry.getKey());
                    continue;
                }
                existingTables.put(entry.getKey(), existing);
            } catch (Catalog.TableNotExistException e) {
                // Target creation happens only after all schemas and constraints are checked.
            }
            acceptedSchemas.put(entry.getKey(), fromMySql);
        }

        for (Map.Entry<String, List<JdbcSchemasInfo.JdbcSchemaInfo>> source :
                schemasBySource.entrySet()) {
            List<Identifier> sourceMonitored = monitoredTablesBySource.get(source.getKey());
            List<Identifier> sourceExcluded = excludedTablesBySource.get(source.getKey());
            for (JdbcSchemasInfo.JdbcSchemaInfo table : source.getValue()) {
                if (acceptedSchemas.containsKey(nameConverter.convert(table.identifier()))) {
                    sourceMonitored.add(table.identifier());
                } else {
                    sourceExcluded.add(table.identifier());
                }
            }
            checkArgument(
                    !sourceMonitored.isEmpty(),
                    "No tables remain to be synchronized for MySQL source '%s' after schema validation.",
                    source.getKey());
            monitoredTables.addAll(sourceMonitored);
            excludedTables.addAll(sourceExcluded);
        }

        for (Map.Entry<String, Schema> entry : acceptedSchemas.entrySet()) {
            Identifier identifier = Identifier.create(database, entry.getKey());
            FileStoreTable table = existingTables.get(entry.getKey());
            if (table == null) {
                catalog.createTable(identifier, entry.getValue(), false);
                table = (FileStoreTable) catalog.getTable(identifier);
            } else {
                table = alterTableOptions(identifier, table);
            }
            tables.add(table);
        }
    }

    @VisibleForTesting
    protected JdbcSchemasInfo discoverSource(
            MySqlSourceConfigs.SourceConfig source,
            Predicate<String> tablePredicate,
            List<Identifier> sourceExcluded)
            throws Exception {
        return MySqlActionUtils.getMySqlTableInfos(
                source.config(), tablePredicate, sourceExcluded, typeMapping);
    }

    @Override
    protected DataStream<RichCdcMultiplexRecord> buildInput() {
        if (!sourceConfigs.isNamed()) {
            return super.buildInput();
        }

        DataStream<RichCdcMultiplexRecord> input = null;
        for (MySqlSourceConfigs.SourceConfig source : sourceConfigs.sources()) {
            MySqlSource<CdcSourceRecord> mySqlSource =
                    MySqlActionUtils.buildMySqlSource(
                            source.config(),
                            tableList(
                                    mode,
                                    source.config().get(MySqlSourceOptions.DATABASE_NAME),
                                    includingTables,
                                    monitoredTablesBySource.get(source.id()),
                                    excludedTablesBySource.get(source.id())),
                            typeMapping);
            DataStreamSource<CdcSourceRecord> stream =
                    buildDataStreamSource(mySqlSource, "MySQL Source " + source.id());
            stream.uid("mysql-source-" + source.id())
                    .setParallelism(source.parallelism(env.getParallelism()));
            DataStream<RichCdcMultiplexRecord> parsed =
                    stream.forward()
                            .flatMap(
                                    MySqlMultiSourceSupport.recordParser(
                                                    source,
                                                    computedColumns,
                                                    typeMapping,
                                                    metadataConverters,
                                                    caseSensitive)
                                            .withTableFilter(sourceTableFilter(source.id())))
                            .name("Parse " + source.id())
                            .uid("mysql-parser-" + source.id())
                            .setParallelism(source.parallelism(env.getParallelism()));
            input = input == null ? parsed : input.union(parsed);
        }
        Map<String, List<DataField>> targetPrimaryKeys = new HashMap<>();
        for (FileStoreTable table : tables) {
            targetPrimaryKeys.put(table.name(), table.schema().primaryKeysFields());
        }
        return MySqlMultiSourceSupport.routeDatabaseRecords(
                Preconditions.checkNotNull(input, "No MySQL sources have been configured."),
                new TableNameConverter(caseSensitive, true, tablePrefix, tableSuffix, tableMapping),
                targetPrimaryKeys,
                caseSensitive,
                env.getParallelism());
    }

    @VisibleForTesting
    SerializablePredicate<Identifier> sourceTableFilter(String sourceId) {
        return MySqlMultiSourceSupport.tableFilter(
                includingDbs,
                excludingDbs,
                includingTables,
                excludingTables,
                excludedTablesBySource.get(sourceId),
                rejectedTargetTables,
                new TableNameConverter(
                        caseSensitive, true, tablePrefix, tableSuffix, tableMapping));
    }

    @Override
    protected NewTableSchemaBuilder newTableSchemaBuilder() {
        NewTableSchemaBuilder schemaBuilder = super.newTableSchemaBuilder();
        return sourceConfigs.isNamed()
                ? MySqlMultiSourceSupport.withPrimaryKeyMode(
                                schemaBuilder, sourceConfigs.isolatePrimaryKeys())
                        .withTargetSchemaValidation(database, catalogLoader())
                : schemaBuilder;
    }

    @Override
    protected CdcTimestampExtractor createCdcTimestampExtractor() {
        return MySqlActionUtils.createCdcTimestampExtractor();
    }

    @Override
    protected MySqlSource<CdcSourceRecord> buildSource() {
        validateRuntimeExecutionMode();
        return MySqlActionUtils.buildMySqlSource(
                cdcSourceConfig,
                tableList(
                        mode,
                        cdcSourceConfig.get(MySqlSourceOptions.DATABASE_NAME),
                        includingTables,
                        monitoredTables,
                        excludedTables),
                typeMapping);
    }

    private void logNonPkTables(List<Identifier> nonPkTables) {
        if (!nonPkTables.isEmpty()) {
            LOG.debug(
                    "Didn't find primary keys for tables '{}'. "
                            + "These tables won't be synchronized.",
                    nonPkTables.stream()
                            .map(Identifier::getFullName)
                            .collect(Collectors.joining(",")));
            excludedTables.addAll(nonPkTables);
        }
    }

    private boolean shouldMonitorTable(
            String mySqlTableName, Pattern includingPattern, @Nullable Pattern excludingPattern) {
        boolean shouldMonitor = includingPattern.matcher(mySqlTableName).matches();
        if (excludingPattern != null) {
            shouldMonitor = shouldMonitor && !excludingPattern.matcher(mySqlTableName).matches();
        }
        if (!shouldMonitor) {
            LOG.debug("Source table '{}' is excluded.", mySqlTableName);
        }
        return shouldMonitor;
    }

    private boolean shouldMonitorTable(
            TableSchema tableSchema, Schema mySqlSchema, Supplier<String> errMsg) {
        if (schemaCompatible(tableSchema, mySqlSchema.fields())) {
            return true;
        } else if (ignoreIncompatible) {
            LOG.warn(errMsg.get() + "This table will be ignored.");
            return false;
        } else {
            throw new IllegalArgumentException(
                    errMsg.get()
                            + "If you want to ignore the incompatible tables, please specify --ignore-incompatible to true.");
        }
    }

    private Supplier<String> incompatibleMessage(
            TableSchema paimonSchema, JdbcTableInfo jdbcTableInfo, Identifier identifier) {
        return () ->
                String.format(
                        "Incompatible schema found.\n"
                                + "Paimon table is: %s, fields are: %s.\n"
                                + "MySQL table is: %s, fields are: %s.\n",
                        identifier.getFullName(),
                        paimonSchema.fields(),
                        jdbcTableInfo.location(),
                        jdbcTableInfo.schema().fields());
    }

    @VisibleForTesting
    public List<Identifier> monitoredTables() {
        return monitoredTables;
    }

    @VisibleForTesting
    public List<Identifier> excludedTables() {
        return excludedTables;
    }

    @VisibleForTesting
    public List<Identifier> monitoredTables(String sourceId) {
        return monitoredTablesBySource.getOrDefault(sourceId, Collections.emptyList());
    }

    @VisibleForTesting
    public List<Identifier> excludedTables(String sourceId) {
        return excludedTablesBySource.getOrDefault(sourceId, Collections.emptyList());
    }

    @Override
    protected boolean requirePrimaryKeys() {
        return true;
    }
}
