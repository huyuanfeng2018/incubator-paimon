---
title: "Mysql CDC"
weight: 2
type: docs
aliases:
- /cdc-ingestion/mysql-cdc.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# MySQL CDC

Paimon supports synchronizing changes from different databases using change data capture (CDC). This feature requires Flink and its [CDC connectors](https://nightlies.apache.org/flink/flink-cdc-docs-stable/).

## Prepare CDC Bundled Jar

Download `CDC Bundled Jar` and put them under <FLINK_HOME>/lib/.

| Version | Bundled Jar                                                                                                             |
|---------|-------------------------------------------------------------------------------------------------------------------------|
| 3.1.x   | <li> flink-sql-connector-mysql-cdc-3.1.x.jar <li> mysql-connector-java-8.0.27.jar                                       |

{{< hint danger >}}
Only cdc 3.1+ is supported.

You can download the `flink-connector-mysql-cdc` jar package by clicking [here](https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-mysql-cdc/).

{{< /hint >}}

## Synchronizing Tables

By using [MySqlSyncTableAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncTableAction) in a Flink DataStream job or directly through `flink run`, users can synchronize one or multiple tables from MySQL into one Paimon table.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table \
    --warehouse <warehouse-path> \
    --database <database-name> \
    --table <table-name> \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--type_mapping <option1,option2...>] \
    [--computed_column <'column-name=expr-name(args[, ...])'> [--computed_column ...]] \
    [--metadata_column <metadata-column>] \
    [--mysql_conf <mysql-cdc-source-conf> [--mysql_conf <mysql-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mysql_sync_table >}}

If the Paimon table you specify does not exist, this action will automatically create the table. Its schema will be derived from all specified MySQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified MySQL tables.

### Example 1: synchronize tables into one Paimon table

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='source_db' \
    --mysql_conf table-name='source_table1|source_table2' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

As example shows, the mysql_conf's table-name supports regular expressions to monitor multiple tables that satisfy
the regular expressions. The schemas of all the tables will be merged into one Paimon table schema.

### Example 2: synchronize shards into one Paimon table

You can also set 'database-name' with a regular expression to capture multiple databases. A typical scenario is that a 
table 'source_table' is split into database 'source_db1', 'source_db2' ..., then you can synchronize data of all the 
'source_table's into one Paimon table.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --table test_table \
    --partition_keys pt \
    --primary_keys pt,uid \
    --computed_column '_year=year(age)' \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='source_db.+' \
    --mysql_conf table-name='source_table' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

## Synchronizing Databases

By using [MySqlSyncDatabaseAction](/docs/{{< param Branch >}}/api/java/org/apache/paimon/flink/action/cdc/mysql/MySqlSyncDatabaseAction) in a Flink DataStream job or directly through `flink run`, users can synchronize the whole MySQL database into one Paimon database.

To use this feature through `flink run`, run the following shell command.

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse <warehouse-path> \
    --database <database-name> \
    [--ignore_incompatible <true/false>] \
    [--merge_shards <true/false>] \
    [--table_prefix <paimon-table-prefix>] \
    [--table_suffix <paimon-table-suffix>] \
    [--including_tables <mysql-table-name|name-regular-expr>] \
    [--excluding_tables <mysql-table-name|name-regular-expr>] \
    [--mode <sync-mode>] \
    [--metadata_column <metadata-column>] \
    [--type_mapping <option1,option2...>] \
    [--partition_keys <partition_keys>] \
    [--primary_keys <primary-keys>] \
    [--mysql_conf <mysql-cdc-source-conf> [--mysql_conf <mysql-cdc-source-conf> ...]] \
    [--catalog_conf <paimon-catalog-conf> [--catalog_conf <paimon-catalog-conf> ...]] \
    [--table_conf <paimon-table-sink-conf> [--table_conf <paimon-table-sink-conf> ...]]
```

{{< generated/mysql_sync_database >}}

Only tables with primary keys will be synchronized.

For each MySQL table to be synchronized, if the corresponding Paimon table does not exist, this action will automatically create the table. Its schema will be derived from all specified MySQL tables. If the Paimon table already exists, its schema will be compared against the schema of all specified MySQL tables.

### Example 1: synchronize entire database

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4
```

### Example 2: synchronize newly added tables under database

Let's say at first a Flink job is synchronizing tables [product, user, address] 
under database `source_db`. The command to submit the job looks like:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4 \
    --including_tables 'product|user|address'
```

At a later point we would like the job to also synchronize tables [order, custom], 
which contains history data. We can achieve this by recovering from the previous
snapshot of the job and thus reusing existing state of the job. The recovered job will 
first snapshot newly added tables, and then continue reading changelog from previous 
position automatically.

The command to recover from previous snapshot and add new tables to synchronize looks like:

```bash
<FLINK_HOME>/bin/flink run \
    --fromSavepoint savepointPath \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name=source_db \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --including_tables 'product|user|address|order|custom'
```

{{< hint info >}}
You can set `--mode combined` to enable synchronizing newly added tables without restarting job.
{{< /hint >}}

### Example 3: synchronize and merge multiple shards

Let's say you have multiple database shards `db1`, `db2`, ... and each database has tables `tbl1`, `tbl2`, .... You can 
synchronize all the `db.+.tbl.+` into tables `test_db.tbl1`, `test_db.tbl2` ... by following command:

```bash
<FLINK_HOME>/bin/flink run \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_database \
    --warehouse hdfs:///path/to/warehouse \
    --database test_db \
    --mysql_conf hostname=127.0.0.1 \
    --mysql_conf username=root \
    --mysql_conf password=123456 \
    --mysql_conf database-name='db.+' \
    --catalog_conf metastore=hive \
    --catalog_conf uri=thrift://hive-metastore:9083 \
    --table_conf bucket=4 \
    --table_conf changelog-producer=input \
    --table_conf sink.parallelism=4 \
    --including_tables 'tbl.+'
```

By setting database-name to a regular expression, the synchronization job will capture all tables under matched databases 
and merge tables of the same name into one table.

{{< hint info >}}
You can set `--merge_shards false` to prevent merging shards. The synchronized tables will be named to 'databaseName_tableName' 
to avoid potential name conflict.
{{< /hint >}}

## Synchronizing Multiple MySQL Instances

Only `mysql_sync_table` accepts named MySQL instances through
`--mysql_conf source.<id>.<option>=<value>`. Each instance has an independent connection,
CDC Source, parser, startup position, and source state within the same Flink job.
Options without a source prefix are shared defaults, and instance options override them.
Duplicate named option keys are rejected. Use either `--mysql_conf` or its legacy
spelling `--mysql-conf` consistently; mixing both is rejected for named sources.
Configure `hostname` separately for every instance; a shared `hostname` cannot be mixed
with named sources. Source IDs may contain letters, digits, underscores, and hyphens.

For example, synchronize shards from two independent instances into one Paimon table:

```bash
<FLINK_HOME>/bin/flink run -p 2 \
    /path/to/paimon-flink-action-{{< version >}}.jar \
    mysql_sync_table \
    --warehouse hdfs:///path/to/warehouse \
    --database ods \
    --table orders \
    --mysql_conf username=cdc_reader \
    --mysql_conf "password=${MYSQL_CDC_PASSWORD}" \
    --mysql_conf 'database-name=trade_.*' \
    --mysql_conf 'table-name=orders_.*' \
    --mysql_conf server-time-zone=Asia/Shanghai \
    --mysql_conf source.shard_a.hostname=mysql-a.example.internal \
    --mysql_conf source.shard_a.port=3306 \
    --mysql_conf source.shard_a.server-id=5400-5403 \
    --mysql_conf source.shard_a.scan.parallelism=1 \
    --mysql_conf source.shard_b.hostname=mysql-b.example.internal \
    --mysql_conf source.shard_b.port=3306 \
    --mysql_conf source.shard_b.server-id=5500-5503 \
    --mysql_conf source.shard_b.scan.parallelism=2 \
    --table_conf bucket=2
```

Each named instance needs a `server-id` or range containing at least as many IDs as its
`scan.parallelism`. The default source parallelism is the job parallelism, or 1 when it
is unset. Overlapping ranges on the same configured hostname and port are rejected.
The bundled CDC connector represents server IDs as signed Java integers, so the maximum
supported ID is `2147483647`. This validation does not detect DNS aliases or IDs used by
other jobs. Configure `scan.startup.specific-offset.*` independently for each instance;
these options cannot be shared defaults.

### Primary keys and source identity

Named mode defaults to `--mysql_conf multi-source.key-mode=isolated`. It adds three
non-null string metadata columns, `source_id`, `database_name`, and `table_name`, and
prefixes the resolved business primary key with these columns. For a source key `id`,
the target key is `(source_id, database_name, table_name, id)`. Identical IDs in different
instances or physical shards therefore remain separate records. Source IDs are logical,
stable identifiers; changing one changes both the stored identity and the source UID.

Physical or computed columns conflicting with these metadata names are rejected. An
existing target must have matching actual primary keys and partition keys, as well as
a compatible schema. Adding an ordinary metadata column to a table whose primary key
is still `id` is insufficient. Use a new target or an explicit migration when changing
key modes.

Set `--mysql_conf multi-source.key-mode=global` only when the business keys are globally
unique across all participating physical tables, or when overlapping keys deliberately
represent the same logical record. This mode preserves the business keys and does not
automatically add identity metadata. Concurrent updates from separate MySQL instances
have no global transaction order or timestamp-based conflict resolution provided by
this feature.

### Table-only scope and physical shards

One job writes to one explicitly named Paimon table. The existing single-instance
`mysql_sync_database` action is unchanged; it rejects `source.*` and `multi-source.*`
options rather than silently reading only one of the requested instances. Named table
synchronization requires `sync_primary_keys_from_source_schema=true`.

The database and table regular expressions are evaluated during startup discovery.
The source then uses a deterministic, literal list of those physical tables. Adding a
physical shard requires restarting the job and validating it during discovery; live
shard discovery and runtime creation of additional target tables are not supported.
Treat changes to the physical source set as a migration requiring separate recovery
validation, not as a promise of transparent savepoint compatibility.

An explicit target primary key must retain every physical source primary-key column.
This check runs before creating a target or changing its options. During ingestion,
primary-key names or types changing on a monitored shard fail the job before that
schema event is emitted. Missing or malformed schema history also fails closed.
Ordinary non-key columns continue to use the existing schema evolution path.

### Ordering and recovery

Each source and its parser use equal parallelism with forward edges. Parsed records
are then partitioned by the final target table and typed target primary key before
entering the existing single-table sink. Source-key validation happens in each source
parser before the merge and partitioning stage.

All sources participate in the same job checkpoint, while retaining independent source
positions. One slow or failed instance can affect the whole job. This does not create
a transactionally consistent snapshot or event order across separate MySQL instances.

Named sources are sorted by their stable IDs and use explicit source/parser UIDs.
Source UID assignment is independent of configuration declaration order. A previous
implementation passed a reordered-configuration savepoint test; the narrowed table-only
implementation must be revalidated end to end before deployment. Adding, removing, or
renaming sources, changing key modes,
migrating an existing legacy job, and rescaling during snapshots require separate
migration validation. When rebuilding a graph whose routing has changed, use an aligned
checkpoint or savepoint; restoration from an unaligned checkpoint containing old channel
state is not covered. Commands without named options retain the legacy single-source path.

## FAQ

1. Chinese characters in records ingested from MySQL are garbled.

* Try to set `env.java.opts: -Dfile.encoding=UTF-8` in `flink-conf.yaml`(Flink version < 1.19) or `config.yaml`(Flink version >= 1.19)
(the option is changed to `env.java.opts.all` since Flink-1.17).

2. Synchronize MySQL table and column comment.

* Synchronize MySQL create table comment to the paimon table, you need to configure `--mysql_conf jdbc.properties.useInformationSchema=true`.
* Synchronize MySQL alter table or column comment to the paimon table, you need to configure `--mysql_conf debezium.include.schema.comments=true`.
