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
import org.apache.paimon.flink.action.ActionBase;
import org.apache.paimon.flink.action.cdc.CdcActionITCaseBase;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.paimon.flink.action.MultiTablesSinkMode.COMBINED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises independent MySQL instances in one Flink job.
 *
 * <p>Normally starts two MySQL 8.0 containers. To use existing local test instances, provide both
 * {@code paimon.mysql-a.port} and {@code paimon.mysql-b.port}; the default test credentials are
 * {@code paimonuser}/{@code paimonpw}. The account must be able to create isolated test databases
 * and read their binlogs.
 */
public class MySqlMultiSourceITCase extends CdcActionITCaseBase {

    private static final String USER = System.getProperty("paimon.mysql.user", "paimonuser");
    private static final String PASSWORD = System.getProperty("paimon.mysql.password", "paimonpw");
    private static final List<String> COLUMNS =
            Arrays.asList("source_id", "database_name", "table_name", "id", "value_text");

    @Nullable private static MySqlContainer mysqlA;
    @Nullable private static MySqlContainer mysqlB;
    private static String hostA;
    private static String hostB;
    private static int portA;
    private static int portB;

    @TempDir Path savepoints;

    private final List<JobClient> jobs = new ArrayList<>();
    private String sourceDatabase;
    @Nullable private Connection connectionA;
    @Nullable private Connection connectionB;

    @BeforeAll
    public static void startMySqlInstances() throws Exception {
        String externalPortA = System.getProperty("paimon.mysql-a.port");
        String externalPortB = System.getProperty("paimon.mysql-b.port");
        if (externalPortA != null || externalPortB != null) {
            assertThat(externalPortA).as("paimon.mysql-a.port").isNotNull();
            assertThat(externalPortB).as("paimon.mysql-b.port").isNotNull();
            hostA = System.getProperty("paimon.mysql-a.hostname", "127.0.0.1");
            hostB = System.getProperty("paimon.mysql-b.hostname", "127.0.0.1");
            portA = Integer.parseInt(externalPortA);
            portB = Integer.parseInt(externalPortB);
            assertThat(hostA + ":" + portA).isNotEqualTo(hostB + ":" + portB);
            return;
        }

        mysqlA = newContainer();
        mysqlB = newContainer();
        try {
            mysqlA.start();
            mysqlB.start();
            grantTestPrivileges(mysqlA);
            grantTestPrivileges(mysqlB);
            hostA = mysqlA.getHost();
            hostB = mysqlB.getHost();
            portA = mysqlA.getDatabasePort();
            portB = mysqlB.getDatabasePort();
        } catch (Exception failure) {
            stopMySqlInstances();
            throw failure;
        }
    }

    private static MySqlContainer newContainer() {
        MySqlContainer container =
                new MySqlContainer(MySqlVersion.V8_0)
                        .withConfigurationOverride("mysql/my.cnf")
                        .withUsername(USER)
                        .withPassword(PASSWORD);
        container.withCommand("--default-time-zone=+00:00");
        return container;
    }

    private static void grantTestPrivileges(MySqlContainer container) throws Exception {
        Container.ExecResult result =
                container.execInContainer(
                        "mysql",
                        "-uroot",
                        "-p" + PASSWORD,
                        "-e",
                        "GRANT ALL PRIVILEGES ON *.* TO '" + USER + "'@'%';");
        assertThat(result.getExitCode()).as(result.getStderr()).isZero();
    }

    @AfterAll
    public static void stopMySqlInstances() {
        try {
            if (mysqlA != null) {
                mysqlA.stop();
            }
        } finally {
            if (mysqlB != null) {
                mysqlB.stop();
            }
        }
    }

    @BeforeEach
    public void createSourceTables() throws SQLException {
        sourceDatabase = "multi_source_" + UUID.randomUUID().toString().replace("-", "");
        connectionA = connect(hostA, portA);
        connectionB = connect(hostB, portB);
        for (Connection connection : Arrays.asList(connectionA, connectionB)) {
            execute(connection, "CREATE DATABASE " + sourceDatabase);
            connection.setCatalog(sourceDatabase);
            execute(connection, "CREATE TABLE orders (id INT PRIMARY KEY, value_text VARCHAR(32))");
        }
        execute(connectionA, "INSERT INTO orders VALUES (1, 'a_initial')");
        execute(connectionB, "INSERT INTO orders VALUES (1, 'b_initial')");
    }

    private static Connection connect(String host, int port) throws SQLException {
        return DriverManager.getConnection(
                "jdbc:mysql://"
                        + host
                        + ":"
                        + port
                        + "/?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                USER,
                PASSWORD);
    }

    @AfterEach
    public void closeJobsAndSourceDatabases() throws Exception {
        Exception failure = null;
        for (JobClient job : jobs) {
            try {
                if (!job.getJobStatus().get(10, TimeUnit.SECONDS).isGloballyTerminalState()) {
                    job.cancel().get(30, TimeUnit.SECONDS);
                }
            } catch (Exception exception) {
                failure = accumulate(failure, exception);
            }
        }
        jobs.clear();
        for (Connection connection : Arrays.asList(connectionA, connectionB)) {
            if (connection != null) {
                try (Connection ignored = connection) {
                    execute(connection, "DROP DATABASE IF EXISTS " + sourceDatabase);
                } catch (Exception exception) {
                    failure = accumulate(failure, exception);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static Exception accumulate(@Nullable Exception previous, Exception exception) {
        if (previous == null) {
            return exception;
        }
        previous.addSuppressed(exception);
        return previous;
    }

    @Test
    @Timeout(240)
    public void testTableSnapshotDmlAndIndependentSchemaChanges() throws Exception {
        JobClient job = startTable(sourceConfiguration(true), null);
        awaitRows(
                job,
                tableName,
                COLUMNS,
                row("shard_a", "orders", 1, "a_initial"),
                row("shard_b", "orders", 1, "b_initial"));
        assertIsolatedPrimaryKey(tableName);

        execute(connectionA, "INSERT INTO orders VALUES (2, 'a_insert')");
        execute(connectionB, "INSERT INTO orders VALUES (2, 'b_insert')");
        execute(connectionA, "UPDATE orders SET value_text = 'a_updated' WHERE id = 1");
        execute(connectionB, "DELETE FROM orders WHERE id = 1");
        awaitRows(
                job,
                tableName,
                COLUMNS,
                row("shard_a", "orders", 1, "a_updated"),
                row("shard_a", "orders", 2, "a_insert"),
                row("shard_b", "orders", 2, "b_insert"));

        execute(connectionA, "DELETE FROM orders WHERE id = 2");
        execute(connectionB, "UPDATE orders SET value_text = 'b_updated' WHERE id = 2");
        execute(connectionB, "INSERT INTO orders VALUES (1, 'b_reinsert')");
        awaitRows(
                job,
                tableName,
                COLUMNS,
                row("shard_a", "orders", 1, "a_updated"),
                row("shard_b", "orders", 1, "b_reinsert"),
                row("shard_b", "orders", 2, "b_updated"));

        List<String> evolvedColumns = new ArrayList<>(COLUMNS);
        evolvedColumns.add("extra");
        execute(connectionA, "ALTER TABLE orders ADD COLUMN extra INT");
        execute(connectionA, "UPDATE orders SET extra = 10 WHERE id = 1");
        awaitRows(
                job,
                tableName,
                evolvedColumns,
                row("shard_a", "orders", 1, "a_updated", 10),
                row("shard_b", "orders", 1, "b_reinsert", null),
                row("shard_b", "orders", 2, "b_updated", null));

        execute(connectionB, "ALTER TABLE orders ADD COLUMN extra INT");
        execute(connectionB, "UPDATE orders SET extra = 20 WHERE id = 2");
        awaitRows(
                job,
                tableName,
                evolvedColumns,
                row("shard_a", "orders", 1, "a_updated", 10),
                row("shard_b", "orders", 1, "b_reinsert", null),
                row("shard_b", "orders", 2, "b_updated", 20));

        execute(connectionA, "ALTER TABLE orders MODIFY COLUMN extra BIGINT");
        execute(connectionA, "UPDATE orders SET extra = 9000000000 WHERE id = 1");
        // A source that still has the narrower schema must not narrow the merged target.
        execute(connectionB, "UPDATE orders SET extra = 21 WHERE id = 2");
        awaitRows(
                job,
                tableName,
                evolvedColumns,
                row("shard_a", "orders", 1, "a_updated", 9000000000L),
                row("shard_b", "orders", 1, "b_reinsert", null),
                row("shard_b", "orders", 2, "b_updated", 21));

        execute(connectionB, "ALTER TABLE orders MODIFY COLUMN extra BIGINT");
        execute(connectionB, "UPDATE orders SET extra = 8000000000 WHERE id = 1");
        awaitRows(
                job,
                tableName,
                evolvedColumns,
                row("shard_a", "orders", 1, "a_updated", 9000000000L),
                row("shard_b", "orders", 1, "b_reinsert", 8000000000L),
                row("shard_b", "orders", 2, "b_updated", 21));
        RowType latest = getFileStoreTable(tableName).schema().logicalRowType();
        assertThat(latest.getTypeAt(latest.getFieldNames().indexOf("extra")))
                .isEqualTo(DataTypes.BIGINT());

        // A change in source parallelism must not reorder updates for the same primary key.
        for (int i = 0; i < 150; i++) {
            execute(connectionA, "UPDATE orders SET value_text = 'a_burst_" + i + "' WHERE id = 1");
            execute(connectionB, "UPDATE orders SET value_text = 'b_burst_" + i + "' WHERE id = 1");
        }
        awaitRows(
                job,
                tableName,
                evolvedColumns,
                row("shard_a", "orders", 1, "a_burst_149", 9000000000L),
                row("shard_b", "orders", 1, "b_burst_149", 8000000000L),
                row("shard_b", "orders", 2, "b_updated", 21));

        // Updating a primary key must delete only this source's old key and insert its new key.
        execute(connectionA, "UPDATE orders SET id = 3, value_text = 'a_pk_moved' WHERE id = 1");
        awaitRows(
                job,
                tableName,
                evolvedColumns,
                row("shard_a", "orders", 3, "a_pk_moved", 9000000000L),
                row("shard_b", "orders", 1, "b_burst_149", 8000000000L),
                row("shard_b", "orders", 2, "b_updated", 21));
        execute(connectionB, "UPDATE orders SET id = 3, value_text = 'b_pk_moved' WHERE id = 1");
        awaitRows(
                job,
                tableName,
                evolvedColumns,
                row("shard_a", "orders", 3, "a_pk_moved", 9000000000L),
                row("shard_b", "orders", 2, "b_updated", 21),
                row("shard_b", "orders", 3, "b_pk_moved", 8000000000L));
    }

    @Test
    @Timeout(240)
    public void testTableSavepointRestoresSourceIdsAfterConfigurationReordering() throws Exception {
        Map<String, String> options = sourceConfiguration(true);
        JobClient job = startTableWithOrderedOptions(options, null);
        awaitRows(
                job,
                tableName,
                COLUMNS,
                row("shard_a", "orders", 1, "a_initial"),
                row("shard_b", "orders", 1, "b_initial"));
        execute(connectionA, "INSERT INTO orders VALUES (2, 'a_before_savepoint')");
        execute(connectionB, "INSERT INTO orders VALUES (2, 'b_before_savepoint')");
        awaitRows(
                job,
                tableName,
                COLUMNS,
                row("shard_a", "orders", 1, "a_initial"),
                row("shard_a", "orders", 2, "a_before_savepoint"),
                row("shard_b", "orders", 1, "b_initial"),
                row("shard_b", "orders", 2, "b_before_savepoint"));

        String savepoint =
                job.stopWithSavepoint(
                                false, savepoints.toUri().toString(), SavepointFormatType.CANONICAL)
                        .get(60, TimeUnit.SECONDS);
        assertThat(savepoint).isNotBlank();

        execute(connectionA, "UPDATE orders SET value_text = 'a_while_stopped' WHERE id = 1");
        execute(connectionB, "DELETE FROM orders WHERE id = 1");
        execute(connectionA, "INSERT INTO orders VALUES (3, 'a_while_stopped')");
        execute(connectionB, "INSERT INTO orders VALUES (3, 'b_while_stopped')");

        List<Map.Entry<String, String>> reversed = new ArrayList<>(options.entrySet());
        Collections.reverse(reversed);
        Map<String, String> reorderedOptions = new LinkedHashMap<>();
        reversed.forEach(entry -> reorderedOptions.put(entry.getKey(), entry.getValue()));
        env.close();
        env =
                streamExecutionEnvironmentBuilder()
                        .streamingMode()
                        .parallelism(2)
                        .checkpointIntervalMs(500)
                        .build();
        JobClient restored = startTableWithOrderedOptions(reorderedOptions, savepoint);
        awaitRows(
                restored,
                tableName,
                COLUMNS,
                row("shard_a", "orders", 1, "a_while_stopped"),
                row("shard_a", "orders", 2, "a_before_savepoint"),
                row("shard_a", "orders", 3, "a_while_stopped"),
                row("shard_b", "orders", 2, "b_before_savepoint"),
                row("shard_b", "orders", 3, "b_while_stopped"));

        execute(connectionA, "DELETE FROM orders WHERE id = 2");
        execute(connectionB, "UPDATE orders SET value_text = 'b_after_restore' WHERE id = 2");
        execute(connectionA, "INSERT INTO orders VALUES (4, 'a_after_restore')");
        execute(connectionB, "INSERT INTO orders VALUES (4, 'b_after_restore')");
        awaitRows(
                restored,
                tableName,
                COLUMNS,
                row("shard_a", "orders", 1, "a_while_stopped"),
                row("shard_a", "orders", 3, "a_while_stopped"),
                row("shard_a", "orders", 4, "a_after_restore"),
                row("shard_b", "orders", 2, "b_after_restore"),
                row("shard_b", "orders", 3, "b_while_stopped"),
                row("shard_b", "orders", 4, "b_after_restore"));
        assertIsolatedPrimaryKey(tableName);
    }

    @Test
    @Timeout(180)
    public void testCombinedDatabaseDiscoversTablesIndependently() throws Exception {
        execute(connectionA, "CREATE TABLE payments (id INT PRIMARY KEY, value_text VARCHAR(32))");
        execute(connectionA, "INSERT INTO payments VALUES (1, 'a_payment')");
        MySqlSyncDatabaseAction action =
                new SyncDatabaseActionBuilder<MySqlSyncDatabaseAction>(
                        MySqlSyncDatabaseAction.class,
                        sourceConfiguration(false)) {}.withTableConfig(tableOptions())
                        .includingTables("orders|payments|invoices")
                        .withMode(COMBINED.configString())
                        .build();
        JobClient job = start(action, null);
        awaitRows(job, "payments", COLUMNS, row("shard_a", "payments", 1, "a_payment"));
        assertIsolatedPrimaryKey("payments");
        awaitRows(
                job,
                "orders",
                COLUMNS,
                row("shard_a", "orders", 1, "a_initial"),
                row("shard_b", "orders", 1, "b_initial"));

        // Observe binlog-only marker writes from both sources before issuing CREATE TABLE.
        execute(connectionA, "INSERT INTO orders VALUES (2, 'a_binlog')");
        execute(connectionB, "INSERT INTO orders VALUES (2, 'b_binlog')");
        awaitRows(
                job,
                "orders",
                COLUMNS,
                row("shard_a", "orders", 1, "a_initial"),
                row("shard_a", "orders", 2, "a_binlog"),
                row("shard_b", "orders", 1, "b_initial"),
                row("shard_b", "orders", 2, "b_binlog"));

        // The same logical table appears later on source B, targeting an existing Paimon table.
        execute(connectionB, "CREATE TABLE payments (id INT PRIMARY KEY, value_text VARCHAR(32))");
        execute(connectionB, "INSERT INTO payments VALUES (1, 'b_payment')");
        awaitRows(
                job,
                "payments",
                COLUMNS,
                row("shard_a", "payments", 1, "a_payment"),
                row("shard_b", "payments", 1, "b_payment"));
        execute(connectionB, "DELETE FROM payments WHERE id = 1");
        awaitRows(job, "payments", COLUMNS, row("shard_a", "payments", 1, "a_payment"));

        // A wholly new target is created from source B's schema-change event at runtime.
        execute(connectionB, "CREATE TABLE invoices (id INT PRIMARY KEY, value_text VARCHAR(32))");
        execute(connectionB, "INSERT INTO invoices VALUES (1, 'b_invoice')");
        awaitRows(job, "invoices", COLUMNS, row("shard_b", "invoices", 1, "b_invoice"));
        assertIsolatedPrimaryKey("invoices");

        execute(connectionA, "CREATE TABLE invoices (id INT PRIMARY KEY, value_text VARCHAR(32))");
        execute(connectionA, "INSERT INTO invoices VALUES (1, 'a_invoice')");
        awaitRows(
                job,
                "invoices",
                COLUMNS,
                row("shard_a", "invoices", 1, "a_invoice"),
                row("shard_b", "invoices", 1, "b_invoice"));
        assertIsolatedPrimaryKey("invoices");
    }

    private Map<String, String> sourceConfiguration(boolean tableSync) {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("username", USER);
        options.put("password", PASSWORD);
        options.put("database-name", sourceDatabase);
        options.put("server-time-zone", "UTC");
        options.put("scan.incremental.snapshot.chunk.size", "2");
        options.put("scan.newly-added-table.enabled", "false");
        if (tableSync) {
            options.put("table-name", "orders");
        }
        options.put("source.shard_a.hostname", hostA);
        options.put("source.shard_a.port", Integer.toString(portA));
        options.put("source.shard_a.server-id", "5400-5403");
        options.put("source.shard_a.scan.parallelism", "1");
        options.put("source.shard_b.hostname", hostB);
        options.put("source.shard_b.port", Integer.toString(portB));
        options.put("source.shard_b.server-id", "6400-6403");
        options.put("source.shard_b.scan.parallelism", "2");
        return options;
    }

    private static Map<String, String> tableOptions() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("bucket", "1");
        options.put("sink.parallelism", "2");
        return options;
    }

    private JobClient startTable(Map<String, String> options, @Nullable String savepoint)
            throws Exception {
        MySqlSyncTableAction action =
                new SyncTableActionBuilder<MySqlSyncTableAction>(
                        MySqlSyncTableAction.class, options) {}.withTableConfig(tableOptions())
                        .build();
        return start(action, savepoint);
    }

    private JobClient startTableWithOrderedOptions(
            Map<String, String> options, @Nullable String savepoint) throws Exception {
        // The CLI builder copies options into a HashMap. Use the public constructor here so that
        // MySqlSourceConfigs actually receives the reversed caller iteration order after restore.
        MySqlSyncTableAction action =
                new MySqlSyncTableAction(
                        database,
                        tableName,
                        Collections.singletonMap("warehouse", warehouse),
                        options);
        action.withTableConfig(tableOptions());
        return start(action, savepoint);
    }

    private JobClient start(ActionBase action, @Nullable String savepoint) throws Exception {
        action.withStreamExecutionEnvironment(env).build();
        StreamGraph graph = env.getStreamGraph();
        assertThat(
                        graph.getSourceIDs().stream()
                                .map(id -> graph.getStreamNode(id).getParallelism())
                                .collect(Collectors.toList()))
                .as("independently configured MySQL source parallelism")
                .containsExactlyInAnyOrder(1, 2);
        if (savepoint != null) {
            // Apply restore settings to the submitted StreamGraph, with no discarded state.
            graph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepoint, false));
        }
        JobClient job = env.executeAsync(graph);
        jobs.add(job);
        return job;
    }

    private void assertIsolatedPrimaryKey(String targetTable) throws Exception {
        assertThat(getFileStoreTable(targetTable).schema().primaryKeys())
                .containsExactly("source_id", "database_name", "table_name", "id");
    }

    private void awaitRows(
            JobClient job, String targetTable, List<String> columns, String... expectedRows)
            throws Exception {
        List<String> expected = new ArrayList<>(Arrays.asList(expectedRows));
        Collections.sort(expected);
        List<String> actual = Collections.emptyList();
        String schemaDescription = "table has not been created";
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            checkJobHasNotFailed(job, targetTable);
            try {
                FileStoreTable table = getFileStoreTable(targetTable).copyWithLatestSchema();
                RowType rowType = table.schema().logicalRowType();
                schemaDescription = rowType.toString();
                if (rowType.getFieldNames().containsAll(columns)) {
                    int[] projection =
                            columns.stream().mapToInt(rowType.getFieldNames()::indexOf).toArray();
                    ReadBuilder builder = table.newReadBuilder().withProjection(projection);
                    TableScan.Plan plan = builder.newScan().plan();
                    actual =
                            getResult(
                                    builder.newRead(),
                                    plan == null ? Collections.emptyList() : plan.splits(),
                                    rowType.project(projection));
                    Collections.sort(actual);
                    if (actual.equals(expected)) {
                        return;
                    }
                }
            } catch (Catalog.TableNotExistException ignored) {
                // COMBINED mode creates tables after observing their source schema.
            }
            Thread.sleep(200);
        }
        checkJobHasNotFailed(job, targetTable);
        assertThat(actual)
                .as(
                        "rows of %s, schema %s, job %s, status %s",
                        targetTable,
                        schemaDescription,
                        job.getJobID(),
                        job.getJobStatus().get(10, TimeUnit.SECONDS))
                .containsExactlyElementsOf(expected);
    }

    private static void checkJobHasNotFailed(JobClient job, String targetTable) throws Exception {
        JobStatus status = job.getJobStatus().get(10, TimeUnit.SECONDS);
        if (status.isGloballyTerminalState()) {
            // Propagate the actual Flink job failure instead of timing out while polling the sink.
            job.getJobExecutionResult().get(10, TimeUnit.SECONDS);
            throw new AssertionError(
                    "Job "
                            + job.getJobID()
                            + " ended as "
                            + status
                            + " while awaiting "
                            + targetTable);
        }
    }

    private String row(String sourceId, String sourceTable, int id, String value) {
        return "+I["
                + sourceId
                + ", "
                + sourceDatabase
                + ", "
                + sourceTable
                + ", "
                + id
                + ", "
                + value
                + "]";
    }

    private String row(
            String sourceId, String sourceTable, int id, String value, @Nullable Number extra) {
        String base = row(sourceId, sourceTable, id, value);
        return base.substring(0, base.length() - 1) + ", " + (extra == null ? "NULL" : extra) + "]";
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        }
    }
}
