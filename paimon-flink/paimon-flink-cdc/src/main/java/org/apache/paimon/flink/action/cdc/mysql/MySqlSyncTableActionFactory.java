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

import org.apache.paimon.flink.action.Action;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;
import org.apache.paimon.flink.action.cdc.SyncTableActionFactoryBase;

import java.util.Optional;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MYSQL_CONF;

/** Factory to create {@link MySqlSyncTableAction}. */
public class MySqlSyncTableActionFactory extends SyncTableActionFactoryBase {

    public static final String IDENTIFIER = "mysql_sync_table";

    @Override
    public String identifier() {
        return IDENTIFIER;
    }

    @Override
    public String cdcConfigIdentifier() {
        return MYSQL_CONF;
    }

    @Override
    public Optional<Action> create(MultipleParameterToolAdapter params) {
        if (params.has(MYSQL_CONF)) {
            MySqlSourceConfigs.validateCliOptions(
                    params.getMultiParameter(MYSQL_CONF),
                    params.getMultiParameter(params.fallback(MYSQL_CONF)));
        }
        return super.create(params);
    }

    @Override
    public MySqlSyncTableAction createAction() {
        return new MySqlSyncTableAction(database, table, this.catalogConfig, this.cdcSourceConfig);
    }

    @Override
    public void printHelp() {
        System.out.println(
                "Action \"mysql_sync_table\" creates a streaming job "
                        + "with one or more Flink MySQL CDC sources and a Paimon table sink to consume CDC events.");
        System.out.println();

        System.out.println("Syntax:");
        System.out.println(
                "  mysql_sync_table \\\n"
                        + "--warehouse <warehouse_path> \\\n"
                        + "--database <database_name> \\\n"
                        + "--table <table_name> \\\n"
                        + "[--partition_keys <partition_keys>] \\\n"
                        + "[--primary_keys <primary_keys>] \\\n"
                        + "[--type_mapping <option1,option2...>] \\\n"
                        + "[--computed_column <'column_name=expr_name(args[, ...])'> [--computed_column ...]] \\\n"
                        + "[--metadata_column <metadata_column>] \\\n"
                        + "[--mysql_conf <mysql_cdc_source_conf> [--mysql_conf <mysql_cdc_source_conf> ...]] \\\n"
                        + "[--catalog_conf <paimon_catalog_conf> [--catalog_conf <paimon_catalog_conf> ...]] \\\n"
                        + "[--table_conf <paimon_table_sink_conf> [--table_conf <paimon_table_sink_conf> ...]]");
        System.out.println();

        System.out.println("Partition keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println(
                "If partition key is not defined and the specified Paimon table does not exist, "
                        + "this action will automatically create an unpartitioned Paimon table.");
        System.out.println();

        System.out.println("Primary keys syntax:");
        System.out.println("  key1,key2,...");
        System.out.println("Primary keys will be derived from MySQL tables if not specified.");
        System.out.println();

        System.out.println(
                "--type_mapping is used to specify how to map MySQL type to Paimon type. Please see the doc for usage.");
        System.out.println();

        System.out.println("Please see doc for usage of --computed_column.");
        System.out.println();

        System.out.println(
                "--metadata_column is used to specify which metadata columns to include in the output schema of the connector. Please see the doc for usage.");
        System.out.println();

        System.out.println("MySQL CDC source conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For multiple instances, use --mysql_conf source.<id>.<key>=<value>. "
                        + "Unprefixed options are shared defaults; every source needs its own hostname and a server-id range. "
                        + "Source IDs must remain stable when restoring a job.");
        System.out.println(
                "Named sources default to --mysql_conf multi-source.key-mode=isolated, which adds "
                        + "source_id,database_name,table_name metadata and prepends them to the resolved primary key. "
                        + "Use multi-source.key-mode=global only when business primary keys are globally unique across all shards.");
        System.out.println(
                "Use source.<id>.scan.parallelism to set reader parallelism independently. "
                        + "Specific binlog offsets must be configured per source. "
                        + "Changing the source set or converting a legacy job requires a separate migration.");
        System.out.println(
                "'hostname', 'username', 'password', 'database-name' and 'table-name' "
                        + "are required configurations, others are optional.");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/#connector-options");
        System.out.println();

        System.out.println("Paimon catalog and table sink conf syntax:");
        System.out.println("  key=value");
        System.out.println(
                "For a complete list of supported configurations, "
                        + "see https://paimon.apache.org/docs/master/maintenance/configurations/");
        System.out.println();

        System.out.println("Examples:");
        System.out.println(
                "  mysql_sync_table \\\n"
                        + "    --warehouse hdfs:///path/to/warehouse \\\n"
                        + "    --database test_db \\\n"
                        + "    --table test_table \\\n"
                        + "    --partition_keys pt \\\n"
                        + "    --primary_keys pt,uid \\\n"
                        + "    --mysql_conf hostname=127.0.0.1 \\\n"
                        + "    --mysql_conf username=root \\\n"
                        + "    --mysql_conf password=123456 \\\n"
                        + "    --mysql_conf database-name=source_db \\\n"
                        + "    --mysql_conf table-name='source_table' \\\n"
                        + "    --catalog_conf metastore=hive \\\n"
                        + "    --catalog_conf uri=thrift://hive-metastore:9083 \\\n"
                        + "    --table_conf bucket=4 \\\n"
                        + "    --table_conf changelog-producer=input \\\n"
                        + "    --table_conf sink.parallelism=4");
    }
}
