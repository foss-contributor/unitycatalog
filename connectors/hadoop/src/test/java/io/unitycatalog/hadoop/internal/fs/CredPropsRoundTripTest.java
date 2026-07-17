package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.auth.GenericStorageCredential;
import java.net.URI;
import java.util.Arrays;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Round-trip test that composes the driver-side serialization and the executor-side deserialization
 * for a multi-credential (prefix-scoped) Delta table. Every other test exercises one side against a
 * hand-built config; this one feeds {@link CredPropsUtil}'s actual output into {@link
 * CredScopedFileSystem#initialize}, so the two halves are validated against each other. A drift in
 * the serialized keyspace (e.g. the writer and reader disagreeing on a key name) fails here even
 * though the per-side tests would still pass.
 *
 * <p>No server or cloud: a fake {@code s3://} filesystem ({@link FakeS3FileSystem}) backs the
 * delegate so {@code FileSystem.get} succeeds locally, and the test asserts on the credential the
 * delegate was configured with.
 */
class CredPropsRoundTripTest {

  private static final UCDeltaTableIdentifier IDENTIFIER =
      UCDeltaTableIdentifier.of("cat", "sch", "tbl");

  @AfterEach
  void reset() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
    CredScopedFileSystem.clearCacheForTesting();
  }

  /** Serializes two prefix-scoped credentials on the driver, then deserializes on the executor. */
  private Configuration serializeTwoCredProps() throws Exception {
    // Driver fetcher: vends two S3 credentials, one per storage prefix.
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            mockMultiCredentialFetcher(
                scopedCred("s3://bucket/a", "ak-a"), scopedCred("s3://bucket/b", "ak-b"));

    // The driver's own Hadoop conf carries the real delegate impl, which CredPropsUtil saves under
    // fs.s3.impl.original before installing CredScopedFileSystem; the executor restores it.
    Configuration driverConf = new Configuration(false);
    driverConf.set("fs.s3.impl", FakeS3FileSystem.class.getName());
    driverConf.set("fs.s3a.impl", FakeS3FileSystem.class.getName());

    Map<String, String> props =
        CredPropsUtil.createDeltaTableCredProps(
            /* renewCredEnabled= */ true,
            /* credScopedFsEnabled= */ true,
            driverConf,
            "s3",
            /* apiClient= */ null,
            "http://uc",
            tokenProvider(),
            IDENTIFIER,
            "s3://bucket",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());

    // The serialized props are what Spark ships to the executor. Rebuild the executor-side conf
    // purely from them (plus cache-disable so each initialize builds a fresh delegate).
    Configuration execConf = new Configuration(false);
    props.forEach(execConf::set);
    execConf.set("fs.s3.impl.disable.cache", "true");
    execConf.set("fs.s3a.impl.disable.cache", "true");
    return execConf;
  }

  private static CredScopedFileSystem initAt(String uri, Configuration execConf) throws Exception {
    CredScopedFileSystem fs = new CredScopedFileSystem();
    fs.initialize(new URI(uri), execConf);
    return fs;
  }

  @Test
  void driverSerializedMultiCredResolvesCorrectCredentialPerPrefixOnExecutor() throws Exception {
    Configuration execConf = serializeTwoCredProps();

    // The multi-cred keyspace was written at the top level with no single credential value keys.
    assertThat(execConf.get(UCHadoopConfConstants.UC_SCOPED_CRED_COUNT_KEY)).isEqualTo("2");
    assertThat(execConf.get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY)).isNull();

    // A URI under prefix a resolves to credential a; under prefix b, to credential b.
    CredScopedFileSystem fsA = initAt("s3://bucket/a/data/file", execConf);
    CredScopedFileSystem fsB = initAt("s3://bucket/b/data/file", execConf);

    assertThat(fsA.getDelegate().getConf().get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY))
        .isEqualTo("ak-a");
    assertThat(fsB.getDelegate().getConf().get(UCHadoopConfConstants.S3A_INIT_ACCESS_KEY))
        .isEqualTo("ak-b");
    assertThat(fsA.getDelegate()).isNotSameAs(fsB.getDelegate());
  }

  @Test
  void executorRejectsUriNotCoveredByAnyVendedPrefix() throws Exception {
    Configuration execConf = serializeTwoCredProps();

    assertThatThrownBy(() -> initAt("s3://bucket/c/data/file", execConf))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vended credential prefix covers location");
  }

  private static GenericStorageCredential scopedCred(String prefix, String accessKey) {
    TemporaryCredentials creds =
        new TemporaryCredentials()
            .awsTempCredentials(
                new AwsCredentials()
                    .accessKeyId(accessKey)
                    .secretAccessKey("sk")
                    .sessionToken("st"))
            // A past expiry keeps the driver's initial cache from masking a re-fetch; irrelevant to
            // serialization but keeps the test deterministic.
            .expirationTime(111L);
    return new GenericStorageCredential(new GenericCredential(creds), prefix);
  }

  private static GenericCredentialFetcher mockMultiCredentialFetcher(
      GenericStorageCredential... creds) {
    GenericCredentialFetcher api = mock(GenericCredentialFetcher.class);
    try {
      when(api.createCredentials()).thenReturn(Arrays.asList(creds));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return api;
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }

  /**
   * Minimal {@code s3://} filesystem backed by the local filesystem, used as the restored delegate
   * so {@code FileSystem.get} succeeds without real S3. Only needs to instantiate and carry its
   * conf; the test asserts on that conf rather than performing IO.
   */
  public static class FakeS3FileSystem extends RawLocalFileSystem {
    @Override
    public String getScheme() {
      return "s3";
    }

    @Override
    protected void checkPath(Path path) {
      // Accept s3:// paths without the local-filesystem scheme/authority validation.
    }
  }
}
