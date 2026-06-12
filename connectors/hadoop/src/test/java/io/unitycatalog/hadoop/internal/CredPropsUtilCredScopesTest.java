package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.auth.ScopedCredential;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Multi-location credential scope emission from {@link CredPropsUtil#fetchDeltaTableCredProps}. */
class CredPropsUtilCredScopesTest {

  private CredPropsUtil.GenericCredentialFetcherFactory originalFactory;

  @BeforeEach
  void saveFactory() {
    originalFactory = CredPropsUtil.genericCredFetcherFactory;
  }

  @AfterEach
  void restoreFactory() {
    CredPropsUtil.genericCredFetcherFactory = originalFactory;
  }

  private static TemporaryCredentials awsCreds(String ak, String sk, String st) {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials().accessKeyId(ak).secretAccessKey(sk).sessionToken(st))
        .expirationTime(Long.MAX_VALUE);
  }

  private static final class StubFetcher implements GenericCredentialFetcher {
    private final List<ScopedCredential> scopes;

    StubFetcher(List<ScopedCredential> scopes) {
      this.scopes = scopes;
    }

    @Override
    public GenericCredential createCredential() {
      return new GenericCredential(awsCreds("cloneAk", "cloneSk", "cloneSt"));
    }

    @Override
    public List<ScopedCredential> additionalScopedCredentials() {
      return scopes;
    }
  }

  private static void stubFetcher(List<ScopedCredential> scopes) {
    CredPropsUtil.genericCredFetcherFactory = (apiClient, conf) -> new StubFetcher(scopes);
  }

  private static Map<String, String> fetch(boolean renewCredEnabled) throws Exception {
    return CredPropsUtil.fetchDeltaTableCredProps(
        renewCredEnabled,
        false,
        new Configuration(false),
        "s3",
        null,
        "http://uc",
        TokenProvider.create(Map.of("type", "static", "token", "t")),
        UCDeltaTableIdentifier.of("cat", "sch", "clone"),
        "s3://clone-bucket/tables/clone",
        UCCredentialHadoopConfs.TableOperation.READ_WRITE,
        Map.of());
  }

  @Test
  void emitsPerBucketKeysAndRenewableScopeForCrossBucketCredential() throws Exception {
    stubFetcher(
        List.of(
            new ScopedCredential(
                "s3://base-bucket/tables/base", "READ", awsCreds("baseAk", "baseSk", "baseSt"))));

    Map<String, String> props = fetch(true);

    // Cross-bucket fallback for plain S3A.
    assertThat(props).containsEntry("fs.s3a.bucket.base-bucket.access.key", "baseAk");
    assertThat(props).containsEntry("fs.s3a.bucket.base-bucket.secret.key", "baseSk");
    assertThat(props).containsEntry("fs.s3a.bucket.base-bucket.session.token", "baseSt");

    // Full scope for the credential-scoped FileSystem. The provider config points at the base
    // prefix with the base entry's operation, so its refreshes re-select the base credential.
    assertThat(props).containsEntry(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, "1");
    String ns = UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX + 0;
    assertThat(props)
        .containsEntry(
            ns + UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX_SUFFIX, "s3://base-bucket/tables/base");
    String propNs = ns + UCHadoopConfConstants.UC_CRED_SCOPE_PROP_SUFFIX;
    assertThat(props)
        .containsEntry(
            propNs + UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://base-bucket/tables/base");
    assertThat(props).containsEntry(propNs + UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ");
    assertThat(props).containsEntry(propNs + UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, "baseAk");
    assertThat(props.get(propNs + UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER)).isNotNull();
  }

  @Test
  void sameBucketScopeSkipsPerBucketKeysButKeepsTheScope() throws Exception {
    stubFetcher(
        List.of(
            new ScopedCredential(
                "s3://clone-bucket/tables/base", "READ", awsCreds("baseAk", "baseSk", "baseSt"))));

    Map<String, String> props = fetch(false);

    // Per-bucket keys for the table's own bucket would shadow its own credentials.
    assertThat(props).doesNotContainKey("fs.s3a.bucket.clone-bucket.access.key");
    // The scope still carries the credential; static mode embeds it as fixed keys.
    String propNs =
        UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX
            + 0
            + UCHadoopConfConstants.UC_CRED_SCOPE_PROP_SUFFIX;
    assertThat(props).containsEntry(propNs + "fs.s3a.access.key", "baseAk");
  }

  @Test
  void noAdditionalScopesLeavesPropsUntouched() throws Exception {
    stubFetcher(List.of());
    Map<String, String> props = fetch(true);
    assertThat(props.keySet())
        .noneMatch(k -> k.startsWith(UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX));
  }
}
