package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies the caching behaviour of {@link CredScopedFileSystem}: same credential scope reuses the
 * same delegate, different scopes get independent instances, and evicted entries are closed.
 *
 * <p>Uses {@code file://} URIs with the local filesystem so no cloud SDK is required.
 */
class CredScopedFileSystemCacheTest {

  @AfterEach
  void clearCache() {
    CredScopedFileSystem.clearCacheForTesting();
  }

  private static CredScopedFileSystem init(URI uri, Configuration conf) throws Exception {
    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(uri, conf);
    return fs;
  }

  private static Configuration tableConf(String tableId, String op) {
    Configuration conf = new Configuration();
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, tableId);
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, op);
    // Disable Hadoop's internal filesystem cache so newFileSystem() creates a fresh instance per
    // credential scope, making it possible to assert identity inequality across scopes.
    conf.set("fs.file.impl.disable.cache", "true");
    return conf;
  }

  @Test
  void sameScopeReusesSameDelegate() throws Exception {
    URI uri = new URI("file:///tmp");
    Configuration conf = tableConf("tid-1", "READ");

    CredScopedFileSystem fs1 = init(uri, conf);
    CredScopedFileSystem fs2 = init(uri, conf);

    assertThat(fs1.getDelegate()).isSameAs(fs2.getDelegate());
  }

  @Test
  void differentScopeGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");

    CredScopedFileSystem fsRead = init(uri, tableConf("tid-1", "READ"));
    CredScopedFileSystem fsWrite = init(uri, tableConf("tid-1", "WRITE"));

    assertThat(fsRead.getDelegate()).isNotSameAs(fsWrite.getDelegate());
  }

  @Test
  void evictedEntryClosesCachedDelegate() throws Exception {
    // Pre-seed the cache with a mock delegate so we can verify close() is called.
    FileSystem mockFs = mock(FileSystem.class);
    CredScopedKey key = new CredScopedKey.TableCredScopedKey("tid-evict", "READ");
    CredScopedFileSystem.CACHE.put(key, mockFs);

    // Invalidate (simulates LRU eviction) and flush the removal listener.
    CredScopedFileSystem.clearCacheForTesting();

    verify(mockFs).close();
  }

  @Test
  void initializeThreadsEffectiveConfIntoKeyAndDelegateScopedDistinctUncoveredBaselineCachedReuse()
      throws Exception {
    Configuration conf = tableConf("tid-1", "WRITE");
    // A scope overlays op=READ for file:///scoped paths; operation is part of the cache key, so a
    // covered path resolves to a different delegate than the table's own (op=WRITE) path.
    Map<String, String> scope = new HashMap<>();
    CredentialScopes.encode(
        scope, 0, "file:///scoped", Map.of(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ"));
    scope.forEach(conf::set);
    conf.set(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, "1");

    CredScopedFileSystem covered1 = init(new URI("file:///scoped/data/part-0"), conf);
    CredScopedFileSystem covered2 = init(new URI("file:///scoped/data/part-1"), conf);
    CredScopedFileSystem ownPath = init(new URI("file:///own/x"), conf);

    assertThat(covered1.getDelegate()).isNotSameAs(ownPath.getDelegate());
    assertThat(covered1.getDelegate()).isSameAs(covered2.getDelegate());
    // The caller's conf is overlaid onto a copy, not mutated.
    assertThat(conf.get(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY)).isEqualTo("WRITE");
  }
}
