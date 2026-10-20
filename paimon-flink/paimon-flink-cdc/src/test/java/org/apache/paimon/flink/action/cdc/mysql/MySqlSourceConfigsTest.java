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

import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.config.ServerIdRange;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link MySqlSourceConfigs}. */
class MySqlSourceConfigsTest {

    @ParameterizedTest
    @ValueSource(strings = {"hostname", "password", "database-name", "scan.parallelism"})
    void testDuplicateNamedCliKeysAreRejectedWithoutValues(String option) {
        String key = "source.a." + option;
        assertThatThrownBy(
                        () ->
                                MySqlSourceConfigs.validateNoDuplicateNamedOptions(
                                        Arrays.asList(
                                                key + "=first-sensitive=value",
                                                " " + key + " =second-sensitive=value")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duplicate named MySQL option key")
                .hasMessageContaining(key)
                .hasMessageNotContaining("first-sensitive")
                .hasMessageNotContaining("second-sensitive")
                .hasNoCause();
    }

    @Test
    void testDistinctNamedKeysAndDuplicateDefaultsRemainValid() {
        assertThatCode(
                        () ->
                                MySqlSourceConfigs.validateNoDuplicateNamedOptions(
                                        Arrays.asList(
                                                "hostname=mysql-first",
                                                "hostname=mysql-last",
                                                "username=user-first",
                                                "username=user-last",
                                                "source.a.hostname=mysql-a",
                                                "source.b.hostname=mysql-b",
                                                "source.a.password=secret=a",
                                                "source.b.password=secret=b")))
                .doesNotThrowAnyException();
    }

    @Test
    void testLegacyConfigurationIsPreservedWithoutAdditionalValidation() {
        Map<String, String> options = new HashMap<>();
        options.put("hostname", "legacy-mysql");
        options.put("server-id", "  a-legacy-value  ");
        options.put("database-name", "[");
        options.put("scan.parallelism", "a-legacy-value");
        options.put("scan.startup.specific-offset.file", "mysql-bin.000001");

        MySqlSourceConfigs configs = MySqlSourceConfigs.fromMap(options);
        assertThat(configs.isNamed()).isFalse();
        assertThat(configs.isolatePrimaryKeys()).isFalse();
        assertThat(configs.sources())
                .extracting(MySqlSourceConfigs.SourceConfig::id)
                .containsExactly("default");
        assertThat(configs.firstConfig().toMap()).isEqualTo(options);
        assertThatCode(() -> configs.validate(100)).doesNotThrowAnyException();
    }

    @Test
    void testEmptyLegacyConfiguration() {
        MySqlSourceConfigs configs = MySqlSourceConfigs.fromMap(new HashMap<>());
        assertThat(configs.firstConfig().toMap()).isEmpty();
        assertThatCode(() -> configs.validate(1)).doesNotThrowAnyException();
    }

    @Test
    void testSharedDefaultsAndSourceOverridesAreIndependent() {
        Map<String, String> options = tableOptions();
        options.put("port", "3307");
        options.put("server-time-zone", "Asia/Shanghai");
        options.put("scan.parallelism", "2");
        options.put("source.b.hostname", "mysql-b");
        options.put("source.b.server-id", "6001-6004");
        options.put("source.b.password", "source-b-password");
        options.put("source.b.database-name", "orders_b_.*");
        options.put("source.b.scan.parallelism", "4");
        options.put("source.a.server-id", "5001-5002");
        options.put("source.a.jdbc.properties.characterEncoding", "UTF-8");

        MySqlSourceConfigs configs = MySqlSourceConfigs.fromMap(options);
        configs.validate(8);

        assertThat(configs.isNamed()).isTrue();
        assertThat(configs.isolatePrimaryKeys()).isTrue();
        assertThat(configs.sources())
                .extracting(MySqlSourceConfigs.SourceConfig::id)
                .containsExactly("a", "b");
        MySqlSourceConfigs.SourceConfig a = configs.sources().get(0);
        MySqlSourceConfigs.SourceConfig b = configs.sources().get(1);
        assertThat(a.config().toMap())
                .containsEntry("hostname", "mysql-a")
                .containsEntry("username", "user")
                .containsEntry("password", "shared-password")
                .containsEntry("port", "3307")
                .containsEntry("table-name", "orders_.*")
                .containsEntry("server-time-zone", "Asia/Shanghai")
                .containsEntry("jdbc.properties.characterEncoding", "UTF-8");
        assertThat(b.config().toMap())
                .containsEntry("hostname", "mysql-b")
                .containsEntry("password", "source-b-password")
                .containsEntry("database-name", "orders_b_.*")
                .doesNotContainKey("jdbc.properties.characterEncoding");
        assertThat(a.parallelism(8)).isEqualTo(2);
        assertThat(b.parallelism(8)).isEqualTo(4);
        assertThat(a.config().toMap())
                .doesNotContainKeys("scan.parallelism", "multi-source.key-mode");
        assertThat(b.config().toMap())
                .doesNotContainKeys("scan.parallelism", "multi-source.key-mode");
        assertThat(configs.firstConfig().toMap()).isEqualTo(a.config().toMap());
    }

    @Test
    void testReturnedConfigurationsAndInputCannotMutateSources() {
        Map<String, String> options = tableOptions();
        MySqlSourceConfigs configs = MySqlSourceConfigs.fromMap(options);
        options.put("source.a.hostname", "changed-input");
        Configuration returned = configs.firstConfig();
        returned.setString("hostname", "changed-output");
        configs.sources().get(0).config().setString("password", "changed-password");

        assertThat(configs.firstConfig().toMap())
                .containsEntry("hostname", "mysql-a")
                .containsEntry("password", "shared-password");
        assertThatThrownBy(() -> configs.sources().clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testKeyMode() {
        Map<String, String> options = tableOptions();
        options.put("multi-source.key-mode", "global");
        MySqlSourceConfigs configs = MySqlSourceConfigs.fromMap(options);
        assertThat(configs.isolatePrimaryKeys()).isFalse();
        assertThat(configs.firstConfig().toMap()).doesNotContainKey("multi-source.key-mode");

        options.put("multi-source.key-mode", "isolated");
        assertThat(MySqlSourceConfigs.fromMap(options).isolatePrimaryKeys()).isTrue();
        options.put("multi-source.key-mode", "sensitive-invalid-value");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multi-source.key-mode")
                .hasMessageNotContaining("sensitive-invalid-value");
    }

    @Test
    void testKeyModeRequiresNamedSources() {
        Map<String, String> options = new HashMap<>();
        options.put("multi-source.key-mode", "global");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("multi-source.key-mode")
                .hasMessageContaining("requires named MySQL sources");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "source.",
                "source.a",
                "source..hostname",
                "source.a.",
                "source.a..hostname",
                "source.a b.hostname",
                "source.a/b.hostname",
                "source.a.hostname.",
                "source.a.source.b.hostname",
                "source.a.multi-source.key-mode"
            })
    void testMalformedSourceKeys(String key) {
        Map<String, String> options = tableOptions();
        options.put(key, "sensitive-invalid-value");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageNotContaining("sensitive-invalid-value")
                .hasNoCause();
    }

    @Test
    void testEachSourceRequiresAnExplicitHostname() {
        Map<String, String> options = tableOptions();
        options.remove("source.a.hostname");
        MySqlSourceConfigs configs = MySqlSourceConfigs.fromMap(options);
        assertThat(configs.firstConfig().toMap()).doesNotContainKey("hostname");
        assertThatThrownBy(() -> configs.validate(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source 'a'")
                .hasMessageContaining("hostname");
    }

    @Test
    void testLegacyAndNamedHostnamesCannotBeMixed() {
        Map<String, String> options = tableOptions();
        options.put("hostname", "legacy-instance-must-not-be-ignored");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("hostname")
                .hasMessageContaining("cannot be mixed")
                .hasMessageNotContaining("legacy-instance-must-not-be-ignored");
    }

    @ParameterizedTest
    @ValueSource(strings = {"username", "password", "database-name", "table-name"})
    void testMissingRequiredOptions(String key) {
        Map<String, String> options = tableOptions();
        options.remove(key);
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source 'a'")
                .hasMessageContaining(key)
                .hasMessageNotContaining("shared-password");
    }

    @Test
    void testExplicitEmptyPasswordIsValid() {
        Map<String, String> options = tableOptions();
        options.put("password", "");
        assertThatCode(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"database-name", "table-name"})
    void testInvalidRegexDoesNotExposeValues(String key) {
        Map<String, String> options = tableOptions();
        options.put(key, "[sensitive-invalid-regex");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(key)
                .hasMessageNotContaining("sensitive-invalid-regex")
                .hasMessageNotContaining("shared-password")
                .hasNoCause();
    }

    @ParameterizedTest
    @ValueSource(strings = {"file", "pos", "gtid-set", "skip-events", "skip-rows"})
    void testSpecificOffsetsMustBeConfiguredForEachSource(String option) {
        Map<String, String> options = tableOptions();
        String key = "scan.startup.specific-offset." + option;
        options.put(key, "sensitive-offset");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(key)
                .hasMessageNotContaining("sensitive-offset");

        options.remove(key);
        options.put("source.a." + key, "source-a-offset");
        MySqlSourceConfigs configs = MySqlSourceConfigs.fromMap(options);
        assertThat(configs.firstConfig().toMap()).containsEntry(key, "source-a-offset");
    }

    @Test
    void testServerIdsAreRequired() {
        Map<String, String> options = tableOptions();
        options.remove("source.a.server-id");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source 'a'")
                .hasMessageContaining("server-id");
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "2147483647", "2147483640-2147483647", "1-2147483647"})
    void testServerIdsSupportedByTheBundledConnector(String value) {
        Map<String, String> options = tableOptions();
        options.put("source.a.server-id", value);
        assertThatCode(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .doesNotThrowAnyException();
        assertThatCode(() -> ServerIdRange.from(value)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                " 5401",
                "5401 ",
                " 5401 ",
                "\t5401\n",
                " 5401-5402 ",
                " 5401 - 5402 ",
                "\t5401\t-\n5402\n"
            })
    void testNamedServerIdWhitespaceIsNormalizedForTheConnector(String value) {
        for (String key : Arrays.asList("server-id", "source.a.server-id")) {
            Map<String, String> options = tableOptions();
            options.remove("source.a.server-id");
            options.put(key, value);
            Map<String, String> original = new HashMap<>(options);

            MySqlSourceConfigs configs = MySqlSourceConfigs.fromMap(options);
            configs.validate(1);
            String propagated = configs.sources().get(0).config().get(MySqlSourceOptions.SERVER_ID);
            assertThat(propagated).isEqualTo(value.trim());
            assertThatCode(() -> ServerIdRange.from(propagated)).doesNotThrowAnyException();
            assertThat(options).isEqualTo(original);
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "",
                "0",
                "-1",
                "2147483648",
                "4294967295",
                "4294967296",
                "0-1",
                "100-99",
                "1-2147483648",
                "1-4294967296",
                "99999999999999999999999",
                "1-2-3",
                "1,2",
                " sensitive-invalid-value ",
                "sensitive-invalid-value"
            })
    void testInvalidServerIdRanges(String value) {
        Map<String, String> options = tableOptions();
        options.put("source.a.server-id", value);
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source 'a'")
                .hasMessageContaining("server-id")
                .hasMessageNotContaining("sensitive-invalid-value")
                .hasNoCause();
    }

    @Test
    void testServerIdRangeCoversEffectiveParallelism() {
        Map<String, String> options = tableOptions();
        options.put("source.a.server-id", "5001-5002");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options).validate(3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("server-id")
                .hasMessageContaining("parallelism");

        options.put("source.a.scan.parallelism", "2");
        assertThatCode(() -> MySqlSourceConfigs.fromMap(options).validate(3))
                .doesNotThrowAnyException();

        options.remove("source.a.scan.parallelism");
        assertThat(MySqlSourceConfigs.fromMap(options).sources().get(0).parallelism(-1))
                .isEqualTo(1);
        assertThatCode(() -> MySqlSourceConfigs.fromMap(options).validate(-1))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "1.5", "2147483648", "sensitive-invalid-value"})
    void testInvalidParallelism(String value) {
        Map<String, String> options = tableOptions();
        options.put("source.a.scan.parallelism", value);
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("scan.parallelism")
                .hasMessageNotContaining("sensitive-invalid-value")
                .hasNoCause();
    }

    @Test
    void testServerIdRangesCannotOverlapOnTheSameEndpoint() {
        Map<String, String> options = tableOptions();
        options.put("source.a.server-id", "5001-5004");
        options.put("source.b.hostname", "MYSQL-A");
        options.put("source.b.port", "3306");
        options.put("source.b.server-id", "5004-5008");
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("'a' and 'b'")
                .hasMessageContaining("overlapping 'server-id'")
                .hasMessageNotContaining("MYSQL-A")
                .hasMessageNotContaining("5004")
                .hasMessageNotContaining("shared-password");

        options.put("source.b.server-id", "5005-5008");
        assertThatCode(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .doesNotThrowAnyException();
    }

    @Test
    void testDifferentEndpointsMayReuseServerIds() {
        Map<String, String> options = tableOptions();
        options.put("source.b.hostname", "mysql-b");
        options.put("source.b.server-id", "5001");
        assertThatCode(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .doesNotThrowAnyException();

        options.put("source.b.hostname", "mysql-a");
        options.put("source.b.port", "3307");
        assertThatCode(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .doesNotThrowAnyException();
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "-1", "65536", "sensitive-invalid-value"})
    void testInvalidPortDoesNotExposeValues(String value) {
        Map<String, String> options = tableOptions();
        options.put("source.a.port", value);
        assertThatThrownBy(() -> MySqlSourceConfigs.fromMap(options).validate(1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port")
                .hasMessageNotContaining("sensitive-invalid-value")
                .hasNoCause();
    }

    private static Map<String, String> tableOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("username", "user");
        options.put("password", "shared-password");
        options.put("database-name", "orders_db_.*");
        options.put("table-name", "orders_.*");
        options.put("source.a.hostname", "mysql-a");
        options.put("source.a.server-id", "5001");
        return options;
    }
}
