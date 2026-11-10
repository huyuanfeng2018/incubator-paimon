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

package org.apache.paimon.catalog;

import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.options.Options;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/** Tests for typed lookup of existing catalogs through wrappers. */
class CatalogUnwrapTest {

    @TempDir java.nio.file.Path tempDir;

    @Test
    void testDefaultUnwrapReturnsItself() {
        FileSystemCatalog catalog = createCatalog();

        assertThat(catalog.unwrap(FileSystemCatalog.class)).containsSame(catalog);
        assertThat(catalog.unwrap(AbstractCatalog.class)).containsSame(catalog);
        assertThat(catalog.unwrap(Catalog.class)).containsSame(catalog);
        assertThat(catalog.unwrap(CachingCatalog.class)).isEmpty();
    }

    @Test
    void testUnwrapThroughCachingCatalog() {
        FileSystemCatalog catalog = createCatalog();
        CachingCatalog caching = new CachingCatalog(catalog, new Options());

        assertThat(caching.unwrap(FileSystemCatalog.class)).containsSame(catalog);
        assertThat(caching.unwrap(CachingCatalog.class)).containsSame(caching);
    }

    @Test
    void testUnwrapThroughMultipleDelegates() {
        FileSystemCatalog catalog = createCatalog();
        CachingCatalog caching =
                new CachingCatalog(new TestDelegateCatalog(catalog), new Options());
        Catalog outer = new TestDelegateCatalog(caching);

        assertThat(outer.unwrap(FileSystemCatalog.class)).containsSame(catalog);
        assertThat(outer.unwrap(CachingCatalog.class)).containsSame(caching);
        assertThat(outer.unwrap(Catalog.class)).containsSame(outer);
    }

    @Test
    void testUnwrapReturnsFirstMatchingWrapper() {
        CachingCatalog inner = new CachingCatalog(createCatalog(), new Options());
        CachingCatalog outer = new CachingCatalog(new TestDelegateCatalog(inner), new Options());

        assertThat(outer.unwrap(CachingCatalog.class)).containsSame(outer);
        assertThat(outer.unwrap(DelegateCatalog.class)).containsSame(outer);
    }

    @Test
    void testSelfMatchDoesNotInspectWrappedCatalog() {
        Catalog wrapped = mock(Catalog.class);
        TestDelegateCatalog outer = new TestDelegateCatalog(wrapped);

        assertThat(outer.unwrap(TestDelegateCatalog.class)).containsSame(outer);
        assertThat(outer.unwrap(Catalog.class)).containsSame(outer);
        verifyNoInteractions(wrapped);
    }

    @Test
    void testUnwrapReturnsEmptyWhenTypeIsAbsent() {
        Catalog catalog = new TestDelegateCatalog(new TestDelegateCatalog(createCatalog()));

        assertThat(catalog.unwrap(CachingCatalog.class)).isEmpty();
    }

    @Test
    void testUnwrapPreservesStateAndLifecycle() throws Exception {
        FileSystemCatalog catalog = spy(createCatalog());
        catalog.createDatabase("existing", false);
        CachingCatalog caching = new CachingCatalog(catalog, new Options());
        Database cachedDatabase = caching.getDatabase("existing");
        Catalog outer = new TestDelegateCatalog(caching);
        clearInvocations(catalog);

        assertThat(outer.unwrap(FileSystemCatalog.class)).containsSame(catalog);
        assertThat(outer.unwrap(CachingCatalog.class)).containsSame(caching);
        assertThat(caching.getDatabase("existing")).isSameAs(cachedDatabase);
        verify(catalog, never()).getDatabase("existing");
        verify(catalog, never()).catalogLoader();
        verify(catalog, never()).close();
    }

    @Test
    void testUnwrapRejectsNullType() {
        Catalog catalog = createCatalog();

        assertThatThrownBy(() -> catalog.unwrap(null)).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new TestDelegateCatalog(catalog).unwrap(null))
                .isInstanceOf(NullPointerException.class);
    }

    private FileSystemCatalog createCatalog() {
        return new FileSystemCatalog(LocalFileIO.create(), new Path(tempDir.toUri()));
    }

    private static class TestDelegateCatalog extends DelegateCatalog {

        private TestDelegateCatalog(Catalog wrapped) {
            super(wrapped);
        }

        @Override
        public CatalogLoader catalogLoader() {
            return wrapped.catalogLoader();
        }
    }
}
