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

package org.apache.paimon.flink.clone;

import org.apache.paimon.catalog.CachingCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.FileSystemCatalog;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/** Tests Hive catalog lookup while building clone jobs. */
class CloneHiveTableUtilsTest {

    @TempDir java.nio.file.Path warehouse;

    @Test
    void testCatalogUnwrapOverride() {
        Catalog catalog = mock(Catalog.class);
        HiveCatalog hiveCatalog = mock(HiveCatalog.class);
        when(catalog.unwrap(HiveCatalog.class)).thenReturn(Optional.of(hiveCatalog));

        assertThat(CloneHiveTableUtils.getRootHiveCatalog(catalog)).isSameAs(hiveCatalog);
        assertThat(
                        CloneHiveTableUtils.getRootHiveCatalog(
                                new CachingCatalog(catalog, new Options())))
                .isSameAs(hiveCatalog);
        verify(catalog, times(2)).unwrap(HiveCatalog.class);
        verifyNoMoreInteractions(catalog, hiveCatalog);
    }

    @Test
    void testMissingHiveCatalogReportsRootType() {
        Catalog catalog =
                new CachingCatalog(
                        new FileSystemCatalog(LocalFileIO.create(), new Path(warehouse.toUri())),
                        new Options());

        assertThatThrownBy(() -> CloneHiveTableUtils.getRootHiveCatalog(catalog))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "Only support HiveCatalog now but found "
                                + FileSystemCatalog.class.getName()
                                + ".");
    }
}
