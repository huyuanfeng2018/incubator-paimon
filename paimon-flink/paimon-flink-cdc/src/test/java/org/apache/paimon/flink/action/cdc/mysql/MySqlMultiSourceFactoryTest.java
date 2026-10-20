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

import org.apache.paimon.flink.action.ActionFactory;
import org.apache.paimon.flink.action.MultipleParameterToolAdapter;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;

import static org.apache.paimon.flink.action.cdc.CdcActionCommonUtils.MYSQL_CONF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** CLI safeguards for the MySQL table and database action factories. */
class MySqlMultiSourceFactoryTest {

    @ParameterizedTest
    @ValueSource(strings = {"--mysql_conf", "--mysql-conf"})
    void testDuplicateNamedHostnamesFailBeforeCreatingAction(String flag) {
        MultipleParameterToolAdapter params =
                new MultipleParameterToolAdapter(
                        new String[] {
                            flag, "source.a.hostname=first-instance",
                            flag, "source.a.hostname=second-instance"
                        });

        for (ActionFactory factory : factories()) {
            // No catalog or MySQL connection is needed to reject an ambiguous source definition.
            assertThatThrownBy(() -> factory.create(params))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Duplicate named MySQL option key 'source.a.hostname'")
                    .hasMessageNotContaining("first-instance")
                    .hasMessageNotContaining("second-instance");
        }
    }

    @Test
    void testEvenIdenticalNamedOptionsCannotBeRepeated() {
        MultipleParameterToolAdapter params =
                new MultipleParameterToolAdapter(
                        new String[] {
                            "--mysql_conf", "source.a.password=sensitive=value",
                            "--mysql_conf", "source.a.password=sensitive=value"
                        });
        assertThat(params.getMultiParameter(MYSQL_CONF)).hasSize(2);
        for (ActionFactory factory : factories()) {
            assertThatThrownBy(() -> factory.create(params))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Duplicate named MySQL option key 'source.a.password'")
                    .hasMessageNotContaining("sensitive=value");
        }
    }

    @ParameterizedTest
    @CsvSource({
        "source.a.hostname=first-instance, source.b.hostname=second-instance",
        "source.a.hostname=same-instance, source.a.hostname=same-instance",
        "hostname=legacy-instance, source.a.hostname=named-instance",
        "source.a.hostname=named-instance, hostname=legacy-instance"
    })
    void testMixedSpellingsCannotSilentlyDiscardNamedSources(
            String underscoredOption, String hyphenatedOption) {
        MultipleParameterToolAdapter params =
                new MultipleParameterToolAdapter(
                        new String[] {
                            "--mysql_conf", underscoredOption,
                            "--mysql-conf", hyphenatedOption
                        });
        for (ActionFactory factory : factories()) {
            assertThatThrownBy(() -> factory.create(params))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("cannot mix '--mysql_conf' and '--mysql-conf'")
                    .hasMessageContaining("use only one spelling")
                    .hasMessageNotContaining(underscoredOption)
                    .hasMessageNotContaining(hyphenatedOption);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"--mysql_conf", "--mysql-conf"})
    void testOneSpellingWithDistinctSourcesReachesCommonValidation(String flag) {
        MultipleParameterToolAdapter params =
                new MultipleParameterToolAdapter(
                        new String[] {
                            flag, "source.a.hostname=first-instance",
                            flag, "source.b.hostname=second-instance"
                        });
        for (ActionFactory factory : factories()) {
            assertThatThrownBy(() -> factory.create(params))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Argument 'database' is required");
        }
    }

    @Test
    void testLegacyDuplicateOptionsKeepTheExistingLastValue() {
        MultipleParameterToolAdapter params =
                new MultipleParameterToolAdapter(
                        new String[] {
                            "--mysql_conf", "hostname=first-instance",
                            "--mysql_conf", "hostname=last-instance",
                            "--mysql-conf", "hostname=ignored-legacy-instance"
                        });
        for (ActionFactory factory : factories()) {
            assertThat(factory.optionalConfigMap(params, MYSQL_CONF))
                    .containsEntry("hostname", "last-instance");
            // The MySQL preflight accepts legacy duplicates and reaches common argument validation.
            assertThatThrownBy(() -> factory.create(params))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Argument 'database' is required");
        }
    }

    private static List<ActionFactory> factories() {
        return Arrays.asList(
                new MySqlSyncTableActionFactory(), new MySqlSyncDatabaseActionFactory());
    }
}
