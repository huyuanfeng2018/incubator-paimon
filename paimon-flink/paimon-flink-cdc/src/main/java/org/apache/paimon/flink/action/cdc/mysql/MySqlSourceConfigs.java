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
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/** Parses legacy MySQL configuration and independently checkpointed, named MySQL sources. */
public final class MySqlSourceConfigs {

    private static final String SOURCE_PREFIX = "source.";
    private static final String KEY_MODE = "multi-source.key-mode";
    private static final String SCAN_PARALLELISM = "scan.parallelism";
    private static final String SPECIFIC_OFFSET_PREFIX = "scan.startup.specific-offset.";
    private static final Pattern SOURCE_KEY =
            Pattern.compile("source\\.([a-zA-Z0-9_-]+)\\.([a-zA-Z0-9_-]+(?:\\.[a-zA-Z0-9_-]+)*)");
    private static final Pattern SERVER_ID_RANGE =
            Pattern.compile("([0-9]+)(?:\\s*-\\s*([0-9]+))?");
    // The bundled Flink CDC ServerIdRange stores IDs in signed Java integers.
    private static final long MAX_SERVER_ID = Integer.MAX_VALUE;

    private final boolean named;
    private final boolean isolatePrimaryKeys;
    private final List<SourceConfig> sources;

    private MySqlSourceConfigs(
            boolean named, boolean isolatePrimaryKeys, List<SourceConfig> sources) {
        this.named = named;
        this.isolatePrimaryKeys = isolatePrimaryKeys;
        this.sources = Collections.unmodifiableList(new ArrayList<>(sources));
    }

    /** Database synchronization retains its legacy single-instance implementation. */
    static Map<String, String> requireSingleSourceForDatabase(Map<String, String> options) {
        for (String key : options.keySet()) {
            rejectDatabaseOption(key);
        }
        return options;
    }

    static void rejectDatabaseCliOptions(Iterable<String> options) {
        if (options != null) {
            for (String option : options) {
                int separator = option.indexOf('=');
                rejectDatabaseOption(
                        (separator < 0 ? option : option.substring(0, separator)).trim());
            }
        }
    }

    private static void rejectDatabaseOption(String key) {
        if (key.startsWith(SOURCE_PREFIX) || key.startsWith("multi-source.")) {
            throw new IllegalArgumentException(
                    "Named MySQL sources are supported only by mysql_sync_table, not mysql_sync_database.");
        }
    }

    /** Validates both supported CLI spellings without changing the legacy fallback behavior. */
    static void validateCliOptions(
            Iterable<String> selectedOptions, Iterable<String> hyphenatedOptions) {
        // MultipleParameterToolAdapter returns the stored collection directly. When only
        // mysql-conf is present, both calls select that same collection; two different collections
        // mean both spellings were supplied and the adapter would silently ignore mysql-conf.
        if (hyphenatedOptions != null
                && selectedOptions != hyphenatedOptions
                && (containsNamedOptions(selectedOptions)
                        || containsNamedOptions(hyphenatedOptions))) {
            throw new IllegalArgumentException(
                    "Named MySQL sources cannot mix '--mysql_conf' and '--mysql-conf'; "
                            + "use only one spelling.");
        }
        validateNoDuplicateNamedOptions(selectedOptions);
    }

    private static boolean containsNamedOptions(Iterable<String> rawOptions) {
        for (String rawOption : rawOptions) {
            if (rawOption.trim().startsWith(SOURCE_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    /** Checks named CLI keys before the common configuration parser collapses them into a map. */
    static void validateNoDuplicateNamedOptions(Iterable<String> rawOptions) {
        Set<String> namedKeys = new HashSet<>();
        for (String rawOption : rawOptions) {
            int separator = rawOption.indexOf('=');
            if (separator < 0) {
                // Let the common parser retain its validation of malformed key-value arguments.
                continue;
            }
            String key = rawOption.substring(0, separator).trim();
            if (key.startsWith(SOURCE_PREFIX) && !namedKeys.add(key)) {
                throw new IllegalArgumentException(
                        "Duplicate named MySQL option key '"
                                + key
                                + "'. Specify each source option only once.");
            }
        }
    }

    /**
     * Parses {@code source.<id>.<option>} entries and shared defaults. Named sources are ordered by
     * their stable IDs. Without named entries the legacy configuration is preserved unchanged.
     */
    public static MySqlSourceConfigs fromMap(Map<String, String> options) {
        Map<String, String> defaults = new HashMap<>();
        Map<String, Map<String, String>> overrides = new TreeMap<>();
        for (Map.Entry<String, String> entry : options.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(SOURCE_PREFIX)) {
                Matcher matcher = SOURCE_KEY.matcher(key);
                if (!matcher.matches()) {
                    throw new IllegalArgumentException(
                            "Invalid MySQL source option key '"
                                    + key
                                    + "'; expected source.<id>.<option> with an ID containing only "
                                    + "letters, digits, underscores or hyphens.");
                }
                String id = matcher.group(1);
                String option = matcher.group(2);
                if (option.startsWith(SOURCE_PREFIX) || option.startsWith("multi-source.")) {
                    throw invalidOption(id, option, "must not be nested in a source configuration");
                }
                overrides
                        .computeIfAbsent(id, ignored -> new HashMap<>())
                        .put(option, entry.getValue());
            } else if (!KEY_MODE.equals(key)) {
                defaults.put(key, entry.getValue());
            }
        }

        if (overrides.isEmpty()) {
            if (options.containsKey(KEY_MODE)) {
                throw new IllegalArgumentException(
                        "Option '"
                                + KEY_MODE
                                + "' requires named MySQL sources (source.<id>.<option>).");
            }
            return new MySqlSourceConfigs(
                    false,
                    false,
                    Collections.singletonList(new SourceConfig("default", options, null)));
        }

        String keyMode = options.getOrDefault(KEY_MODE, "isolated");
        if (!"isolated".equals(keyMode) && !"global".equals(keyMode)) {
            throw new IllegalArgumentException(
                    "Option '" + KEY_MODE + "' must be either 'isolated' or 'global'.");
        }
        for (String key : defaults.keySet()) {
            if (MySqlSourceOptions.HOSTNAME.key().equals(key)) {
                throw new IllegalArgumentException(
                        "Option 'hostname' cannot be mixed with named MySQL sources; configure "
                                + "source.<id>.hostname for every instance.");
            }
            if (key.startsWith(SPECIFIC_OFFSET_PREFIX)) {
                throw new IllegalArgumentException(
                        "Option '"
                                + key
                                + "' must be configured separately for each named MySQL source.");
            }
            if (key.startsWith("multi-source.")) {
                throw new IllegalArgumentException(
                        "Unknown MySQL action option key '" + key + "'.");
            }
        }

        List<SourceConfig> sources = new ArrayList<>();
        overrides.forEach(
                (id, sourceOverrides) -> {
                    Map<String, String> config = new HashMap<>(defaults);
                    config.putAll(sourceOverrides);
                    config.forEach(
                            (key, value) -> {
                                if (value == null) {
                                    throw invalidOption(id, key, "must not be null");
                                }
                            });
                    String serverId = config.get(MySqlSourceOptions.SERVER_ID.key());
                    if (serverId != null) {
                        // CDC trims range bounds but not a single ID. Pass the same normalized
                        // value that validation accepts, without changing the caller's map.
                        config.put(MySqlSourceOptions.SERVER_ID.key(), serverId.trim());
                    }
                    // These options belong to the action, not to the CDC connector.
                    String parallelism = config.remove(SCAN_PARALLELISM);
                    sources.add(new SourceConfig(id, config, parallelism));
                });
        return new MySqlSourceConfigs(true, "isolated".equals(keyMode), sources);
    }

    public boolean isNamed() {
        return named;
    }

    public boolean isolatePrimaryKeys() {
        return isolatePrimaryKeys;
    }

    public List<SourceConfig> sources() {
        return sources;
    }

    /** Returns a copy for the common action configuration. */
    public Configuration firstConfig() {
        return sources.get(0).config();
    }

    /**
     * Validates named sources before connecting to MySQL. Legacy sources retain their existing
     * validation behavior. Error messages deliberately exclude option values and exception causes,
     * which can otherwise expose credentials or other sensitive connection details.
     */
    public void validate(int defaultParallelism) {
        if (!named) {
            return;
        }

        Map<String, List<ServerIdRange>> endpointRanges = new HashMap<>();
        for (SourceConfig source : sources) {
            Map<String, String> options = source.config.toMap();
            require(source.id, options, MySqlSourceOptions.HOSTNAME.key(), false);
            require(source.id, options, MySqlSourceOptions.USERNAME.key(), false);
            require(source.id, options, MySqlSourceOptions.PASSWORD.key(), true);
            require(source.id, options, MySqlSourceOptions.DATABASE_NAME.key(), false);
            validatePattern(source.id, options, MySqlSourceOptions.DATABASE_NAME.key());
            require(source.id, options, MySqlSourceOptions.TABLE_NAME.key(), false);
            validatePattern(source.id, options, MySqlSourceOptions.TABLE_NAME.key());

            int parallelism = source.parallelism(defaultParallelism);
            ServerIdRange range = parseServerIdRange(source.id, options);
            if (range.end - range.start + 1 < parallelism) {
                throw invalidOption(
                        source.id,
                        MySqlSourceOptions.SERVER_ID.key(),
                        "must provide at least as many IDs as scan.parallelism or the job parallelism");
            }

            int port =
                    parsePositiveInt(
                            source.id,
                            MySqlSourceOptions.PORT.key(),
                            options.getOrDefault(
                                    MySqlSourceOptions.PORT.key(),
                                    String.valueOf(MySqlSourceOptions.PORT.defaultValue())),
                            65535);
            String endpoint =
                    options.get(MySqlSourceOptions.HOSTNAME.key()).trim().toLowerCase(Locale.ROOT)
                            + ":"
                            + port;
            List<ServerIdRange> ranges =
                    endpointRanges.computeIfAbsent(endpoint, ignored -> new ArrayList<>());
            for (ServerIdRange previous : ranges) {
                if (range.start <= previous.end && previous.start <= range.end) {
                    throw new IllegalArgumentException(
                            "MySQL sources '"
                                    + previous.sourceId
                                    + "' and '"
                                    + source.id
                                    + "' have overlapping 'server-id' ranges on the same hostname and port.");
                }
            }
            ranges.add(range);
        }
    }

    private static void require(
            String id, Map<String, String> options, String key, boolean allowEmpty) {
        String value = options.get(key);
        if (value == null || (!allowEmpty && value.trim().isEmpty())) {
            throw invalidOption(id, key, "is required");
        }
    }

    private static void validatePattern(String id, Map<String, String> options, String key) {
        try {
            Pattern.compile(options.get(key));
        } catch (PatternSyntaxException e) {
            throw invalidOption(id, key, "must be a valid regular expression");
        }
    }

    private static ServerIdRange parseServerIdRange(String id, Map<String, String> options) {
        String key = MySqlSourceOptions.SERVER_ID.key();
        require(id, options, key, false);
        Matcher matcher = SERVER_ID_RANGE.matcher(options.get(key).trim());
        if (matcher.matches()) {
            try {
                long start = Long.parseLong(matcher.group(1));
                long end = matcher.group(2) == null ? start : Long.parseLong(matcher.group(2));
                if (start >= 1 && end >= start && end <= MAX_SERVER_ID) {
                    return new ServerIdRange(id, start, end);
                }
            } catch (NumberFormatException e) {
                // Report the option key only, without propagating the invalid value in the cause.
            }
        }
        throw invalidOption(
                id,
                key,
                "must be an ID or an inclusive ascending range between 1 and " + MAX_SERVER_ID);
    }

    private static int parsePositiveInt(String id, String key, String value, int maximum) {
        try {
            int number = Integer.parseInt(value);
            if (number > 0 && number <= maximum) {
                return number;
            }
        } catch (NumberFormatException e) {
            // Do not include the value or the original exception in the validation failure.
        }
        throw invalidOption(id, key, "must be a positive integer within its supported range");
    }

    private static IllegalArgumentException invalidOption(String id, String key, String reason) {
        return new IllegalArgumentException(
                "MySQL source '" + id + "': option '" + key + "' " + reason + ".");
    }

    /** A stable source identity and its independently configurable CDC connection. */
    public static final class SourceConfig {

        private final String id;
        private final Configuration config;
        private final String parallelism;

        private SourceConfig(String id, Map<String, String> config, String parallelism) {
            this.id = id;
            this.config = Configuration.fromMap(config);
            this.parallelism = parallelism;
        }

        public String id() {
            return id;
        }

        public Configuration config() {
            return new Configuration(config);
        }

        /**
         * Resolves the source parallelism, using one when the environment has no concrete default.
         */
        public int parallelism(int defaultParallelism) {
            return parallelism == null
                    ? Math.max(1, defaultParallelism)
                    : parsePositiveInt(id, SCAN_PARALLELISM, parallelism, Integer.MAX_VALUE);
        }
    }

    private static final class ServerIdRange {

        private final String sourceId;
        private final long start;
        private final long end;

        private ServerIdRange(String sourceId, long start, long end) {
            this.sourceId = sourceId;
            this.start = start;
            this.end = end;
        }
    }
}
