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

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.cdc.CdcSourceRecord;
import org.apache.paimon.flink.action.cdc.SyncJobHandler;
import org.apache.paimon.flink.action.cdc.SyncTableActionBase;
import org.apache.paimon.flink.action.cdc.schema.JdbcSchemasInfo;
import org.apache.paimon.flink.action.cdc.schema.JdbcTableInfo;
import org.apache.paimon.flink.action.cdc.watermark.CdcTimestampExtractor;
import org.apache.paimon.flink.sink.cdc.RichCdcMultiplexRecord;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.cdc.ComputedColumnUtils.buildComputedColumns;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * An {@link Action} which synchronize one or multiple MySQL tables into one Paimon table.
 *
 * <p>You should specify MySQL source table in {@code mySqlConfig}. See <a
 * href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#connector-options">document
 * of flink-cdc-connectors</a> for detailed keys and values.
 *
 * <p>If the specified Paimon table does not exist, this action will automatically create the table.
 * Its schema will be derived from all specified MySQL tables. If the Paimon table already exists,
 * its schema will be compared against the schema of all specified MySQL tables.
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
 */
public class MySqlSyncTableAction extends SyncTableActionBase {

    private final MySqlSourceConfigs mySqlSources;
    private final Map<String, Map<Identifier, Schema>> discoveredSchemas = new LinkedHashMap<>();

    public MySqlSyncTableAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            Map<String, String> mySqlConfig) {
        this(database, table, catalogConfig, MySqlSourceConfigs.fromMap(mySqlConfig));
    }

    private MySqlSyncTableAction(
            String database,
            String table,
            Map<String, String> catalogConfig,
            MySqlSourceConfigs mySqlSources) {
        super(
                database,
                table,
                catalogConfig,
                mySqlSources.firstConfig().toMap(),
                SyncJobHandler.SourceType.MYSQL);
        this.mySqlSources = mySqlSources;
    }

    @Override
    protected void validateSourceOptions() {
        if (!mySqlSources.isNamed()) {
            super.validateSourceOptions();
            return;
        }
        mySqlSources.validate(env.getParallelism());
        validateRuntimeExecutionMode();
        checkArgument(
                syncPKeysFromSourceSchema,
                "Named MySQL sources require sync_primary_keys_from_source_schema=true.");
        metadataConverters =
                MySqlMultiSourceSupport.metadataConverters(mySqlSources, metadataConverters);
    }

    @Override
    protected void beforeBuildingSourceSink() throws Exception {
        if (!mySqlSources.isNamed()) {
            super.beforeBuildingSourceSink();
            return;
        }
        // Discover and validate every instance before changing the target table.
        Schema sourceSchema = retrieveSchema();
        computedColumns = buildComputedColumns(computedColumnArgs, sourceSchema.fields());
        Schema expected = buildPaimonSchema(sourceSchema);
        MySqlTableSchemaValidator.validateTargetKeys(sourceSchema, expected, caseSensitive);
        Identifier identifier = Identifier.create(database, table);
        try {
            FileStoreTable existing = (FileStoreTable) catalog.getTable(identifier);
            MySqlMultiSourceSupport.validateExistingTable(
                    existing.schema(), expected, identifier.getFullName());
            fileStoreTable = alterTableOptions(identifier, existing);
        } catch (Catalog.TableNotExistException e) {
            catalog.createTable(identifier, expected, false);
            fileStoreTable = (FileStoreTable) catalog.getTable(identifier);
        }
    }

    @Override
    protected Schema buildPaimonSchema(Schema retrievedSchema) {
        return MySqlMultiSourceSupport.applyPrimaryKeyMode(
                super.buildPaimonSchema(retrievedSchema), mySqlSources.isolatePrimaryKeys());
    }

    @Override
    protected Schema retrieveSchema() throws Exception {
        discoveredSchemas.clear();
        JdbcSchemasInfo allSchemas = new JdbcSchemasInfo();
        for (MySqlSourceConfigs.SourceConfig source : mySqlSources.sources()) {
            JdbcSchemasInfo schemas;
            try {
                schemas = retrieveSchemas(source.config());
                validateMySqlTableInfos(schemas);
            } catch (Exception e) {
                if (!mySqlSources.isNamed()) {
                    throw e;
                }
                throw new IllegalArgumentException(
                        "Cannot discover MySQL source '"
                                + source.id()
                                + "'. Check its connectivity and matching primary-key tables.",
                        e);
            }
            Map<Identifier, Schema> physicalSchemas = new LinkedHashMap<>();
            discoveredSchemas.put(source.id(), physicalSchemas);
            // Source IDs are diagnostic labels here, never part of the physical CDC table filter.
            for (JdbcSchemasInfo.JdbcSchemaInfo schema : schemas.pkTables()) {
                Identifier sourceTable = schema.identifier();
                physicalSchemas.put(sourceTable, schema.schema());
                Identifier diagnosticId =
                        mySqlSources.isNamed()
                                ? Identifier.create(
                                        source.id() + ":" + sourceTable.getDatabaseName(),
                                        sourceTable.getObjectName())
                                : sourceTable;
                allSchemas.addSchema(diagnosticId, schema.schema());
            }
        }
        JdbcTableInfo tableInfo = allSchemas.mergeAll();
        return tableInfo.schema();
    }

    /** Kept separate from graph construction to allow discovery to be validated independently. */
    protected JdbcSchemasInfo retrieveSchemas(Configuration config) throws Exception {
        Pattern tablePattern = Pattern.compile(config.get(MySqlSourceOptions.TABLE_NAME));
        return MySqlActionUtils.getMySqlTableInfos(
                config,
                tableName -> tablePattern.matcher(tableName).matches(),
                new ArrayList<>(),
                typeMapping);
    }

    @Override
    protected DataStream<RichCdcMultiplexRecord> buildInput() {
        if (!mySqlSources.isNamed()) {
            return super.buildInput();
        }
        DataStream<RichCdcMultiplexRecord> input = null;
        for (MySqlSourceConfigs.SourceConfig source : mySqlSources.sources()) {
            Configuration config = source.config();
            MySqlTableSchemaValidator validator =
                    new MySqlTableSchemaValidator(
                            source.id(), discoveredSchemas.get(source.id()), caseSensitive);
            String tableList = validator.tableList();
            DataStream<RichCdcMultiplexRecord> branch =
                    buildDataStreamSource(
                                    MySqlActionUtils.buildMySqlSource(
                                            config, tableList, typeMapping),
                                    "MySQL Source [" + source.id() + "]")
                            .uid("mysql-source-" + source.id())
                            .setParallelism(source.parallelism(env.getParallelism()))
                            .forward()
                            .flatMap(
                                    MySqlMultiSourceSupport.recordParser(
                                            source,
                                            computedColumns,
                                            typeMapping,
                                            metadataConverters,
                                            caseSensitive)
                                            .withTableSchemaValidator(validator))
                            .name("Parse MySQL [" + source.id() + "]")
                            .uid("mysql-parser-" + source.id())
                            .setParallelism(source.parallelism(env.getParallelism()));
            input = input == null ? branch : input.union(branch);
        }
        return MySqlMultiSourceSupport.routeTableRecords(
                input,
                table,
                fileStoreTable.schema().primaryKeysFields(),
                caseSensitive,
                env.getParallelism());
    }

    @Override
    protected MySqlSource<CdcSourceRecord> buildSource() {
        validateRuntimeExecutionMode();
        String tableList =
                String.format(
                        "(%s)\\.(%s)",
                        cdcSourceConfig.get(MySqlSourceOptions.DATABASE_NAME),
                        cdcSourceConfig.get(MySqlSourceOptions.TABLE_NAME));
        return MySqlActionUtils.buildMySqlSource(cdcSourceConfig, tableList, typeMapping);
    }

    @Override
    protected CdcTimestampExtractor createCdcTimestampExtractor() {
        return MySqlActionUtils.createCdcTimestampExtractor();
    }

    private void validateMySqlTableInfos(JdbcSchemasInfo mySqlSchemasInfo) {
        List<Identifier> nonPkTables = mySqlSchemasInfo.nonPkTables();
        checkArgument(
                nonPkTables.isEmpty(),
                "Source tables of MySQL table synchronization job cannot contain table "
                        + "which doesn't have primary keys.\n"
                        + "They are: %s",
                nonPkTables.stream().map(Identifier::getFullName).collect(Collectors.joining(",")));

        checkArgument(
                !mySqlSchemasInfo.pkTables().isEmpty(),
                "No table satisfies the given database name and table name.");
    }
}
