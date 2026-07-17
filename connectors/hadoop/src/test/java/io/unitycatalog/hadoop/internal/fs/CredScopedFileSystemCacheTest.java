package io.unitycatalog.hadoop.internal.fs;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.DelegateFileSystemCacheKey;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.net.URI;
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

  private static final String CONTEXT_A =
      MapIdGenerator.generateId(Map.of("type", "static", "token", "tenant-a"));
  private static final String CONTEXT_B =
      MapIdGenerator.generateId(Map.of("type", "static", "token", "tenant-b"));

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
    return tableConf(tableId, op, EMPTY_CRED_CONTEXT_ID);
  }

  private static Configuration tableConf(String tableId, String op, String credContextId) {
    Configuration conf = new Configuration();
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, credContextId);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_TABLE_ID_KEY, tableId);
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, op);
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
  void sameTableDifferentCredContextGetsDifferentDelegate() throws Exception {
    URI uri = new URI("file:///tmp");

    CredScopedFileSystem fsTenantA = init(uri, tableConf("tid-1", "READ", CONTEXT_A));
    CredScopedFileSystem fsTenantB = init(uri, tableConf("tid-1", "READ", CONTEXT_B));

    assertThat(fsTenantA.getDelegate()).isNotSameAs(fsTenantB.getDelegate());
  }

  @Test
  void evictedEntryClosesCachedDelegate() throws Exception {
    FileSystem mockFs = mock(FileSystem.class);
    DelegateFileSystemCacheKey key =
        new DelegateFileSystemCacheKey(
            new TableCredId(EMPTY_CRED_CONTEXT_ID, "tid-evict", "READ"), null);
    CredScopedFileSystem.CACHE.put(key, mockFs);

    CredScopedFileSystem.clearCacheForTesting();

    verify(mockFs).close();
  }

  // Builds a two-credential (prefix-scoped) conf: the request scope (a Delta table) is written once
  // at the top level; index 0 covers file:///tmp/a, index 1 file:///tmp/b, each with its own
  // location + init credential value under its namespace.
  private static Configuration multiCredConf() {
    Configuration conf = new Configuration();
    conf.set("fs.file.impl.disable.cache", "true");
    conf.setInt(UCHadoopConfConstants.UC_SCOPED_CRED_COUNT_KEY, 2);

    // Top-level CredId scope: the request scope (a Delta table).
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, EMPTY_CRED_CONTEXT_ID);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true");
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "file:///tmp");

    addScopedCred(conf, 0, "file:///tmp/a", "ak-a");
    addScopedCred(conf, 1, "file:///tmp/b", "ak-b");
    return conf;
  }

  private static void addScopedCred(
      Configuration conf, int index, String prefix, String accessKey) {
    String ns = UCHadoopConfConstants.UC_SCOPED_CRED_PREFIX + index + ".";
    conf.set(ns + UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, prefix);
    conf.set(ns + UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, accessKey);
  }

  @Test
  void multiCredSelectsCredentialWhosePrefixCoversUri() throws Exception {
    CredScopedFileSystem fsA = init(new URI("file:///tmp/a/data"), multiCredConf());
    CredScopedFileSystem fsB = init(new URI("file:///tmp/b/data"), multiCredConf());

    // Different covering prefixes -> different scopes -> different delegates.
    assertThat(fsA.getDelegate()).isNotSameAs(fsB.getDelegate());
    // The matched credential's namespaced init key is overlaid onto the top-level key.
    assertThat(fsA.getDelegate().getConf().get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY))
        .isEqualTo("ak-a");
    assertThat(fsB.getDelegate().getConf().get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY))
        .isEqualTo("ak-b");
  }

  @Test
  void multiCredSameCoveringPrefixReusesDelegate() throws Exception {
    CredScopedFileSystem fs1 = init(new URI("file:///tmp/a/data1"), multiCredConf());
    CredScopedFileSystem fs2 = init(new URI("file:///tmp/a/data2"), multiCredConf());

    // Both URIs are covered by the same prefix (file:///tmp/a) -> same scope -> same delegate.
    assertThat(fs1.getDelegate()).isSameAs(fs2.getDelegate());
  }

  @Test
  void multiCredThrowsWhenNoPrefixCoversUri() {
    assertThatThrownBy(() -> init(new URI("file:///tmp/other"), multiCredConf()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vended credential prefix covers location");
  }

  // A two-credential conf whose prefixes are vended with the s3:// scheme (as the UC server does),
  // backed by a local FakeS3 delegate so FileSystem.get succeeds without real S3.
  private static Configuration s3MultiCredConf() {
    Configuration conf = new Configuration();
    conf.set("fs.s3.impl", FakeS3FileSystem.class.getName());
    conf.set("fs.s3a.impl", FakeS3FileSystem.class.getName());
    conf.set("fs.s3.impl.original", FakeS3FileSystem.class.getName());
    conf.set("fs.s3a.impl.original", FakeS3FileSystem.class.getName());
    conf.set("fs.s3.impl.disable.cache", "true");
    conf.set("fs.s3a.impl.disable.cache", "true");
    conf.setInt(UCHadoopConfConstants.UC_SCOPED_CRED_COUNT_KEY, 2);
    conf.set(UCHadoopConfConstants.UC_CRED_CONTEXT_ID_KEY, EMPTY_CRED_CONTEXT_ID);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true");
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket");
    addScopedCred(conf, 0, "s3://bucket/a", "ak-a");
    addScopedCred(conf, 1, "s3://bucket/b", "ak-b");
    return conf;
  }

  @Test
  void s3aUriSelectsCredentialFromS3VendedPrefix() throws Exception {
    // Regression for the scheme-alias bug: the server vends s3:// prefixes, Hadoop accesses via
    // s3a://. Each s3a:// request must still resolve to the credential for its covering prefix.
    CredScopedFileSystem fsA = init(new URI("s3a://bucket/a/part-0.parquet"), s3MultiCredConf());
    CredScopedFileSystem fsB = init(new URI("s3a://bucket/b/part-0.parquet"), s3MultiCredConf());

    assertThat(fsA.getDelegate().getConf().get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY))
        .isEqualTo("ak-a");
    assertThat(fsB.getDelegate().getConf().get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY))
        .isEqualTo("ak-b");
    assertThat(fsA.getDelegate()).isNotSameAs(fsB.getDelegate());
  }

  /** Local-backed fake s3 filesystem so a delegate can be built without real S3. */
  public static class FakeS3FileSystem extends org.apache.hadoop.fs.RawLocalFileSystem {
    @Override
    public String getScheme() {
      return "s3a";
    }

    @Override
    protected void checkPath(org.apache.hadoop.fs.Path path) {}
  }
}
