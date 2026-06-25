package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.auth.ScopedCredential;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class DeltaStorageCredentialUtilTest {

  @Test
  void selectorRejectsMissingResponse() {
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no storage credentials");
    assertThatThrownBy(
            () ->
                DeltaStorageCredentialUtil.selectForLocation(
                    "s3://bucket/t", Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("has no storage credentials");
  }

  @Test
  void selectorRejectsSingleWithoutPrefixMatch() {
    DeltaStorageCredential only = credAt("s3://other-bucket");
    assertThatThrownBy(
            () -> DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", List.of(only)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No UC Delta credential matched");
  }

  @Test
  void selectorRejectsSingleNull() {
    assertThatThrownBy(
            () ->
                DeltaStorageCredentialUtil.selectForLocation(
                    "s3://bucket/t", Collections.singletonList(null)))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("contains null");
  }

  @Test
  void selectorPicksLongestMatchingPrefix() {
    DeltaStorageCredential bucket = credAt("s3://bucket");
    DeltaStorageCredential table = credAt("s3://bucket/t");
    DeltaStorageCredential child = credAt("s3://bucket/t/child");
    assertThat(
            DeltaStorageCredentialUtil.selectForLocation(
                "s3://bucket/t/child/file", Arrays.asList(bucket, table, child)))
        .isSameAs(child);
  }

  @Test
  void selectorMatchesAtPathBoundary() {
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t", "s3://bucket/t")).isTrue();
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t/x", "s3://bucket/t"))
        .isTrue();
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t-other", "s3://bucket/t"))
        .isFalse();
  }

  @Test
  void selectorNormalizesTrailingSlashes() {
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t//", "s3://bucket/t"))
        .isTrue();
    assertThat(DeltaStorageCredentialUtil.prefixCovers("s3://bucket/t", "s3://bucket/t///"))
        .isTrue();
  }

  @Test
  void selectorIgnoresNullAndPrefixlessInMultiResponse() {
    List<DeltaStorageCredential> creds =
        Arrays.asList(null, new DeltaStorageCredential(), credAt("s3://bucket/t"));
    assertThat(DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", creds).getPrefix())
        .isEqualTo("s3://bucket/t");
  }

  @Test
  void selectorThrowsWhenMultiResponseHasNoMatch() {
    List<DeltaStorageCredential> creds =
        Arrays.asList(credAt("s3://other"), credAt("s3://bucket/sibling"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.selectForLocation("s3://bucket/t", creds))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No UC Delta credential matched");
  }

  @Test
  void toTemporaryCredentialsExtractsAwsKeysAndExpiry() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("s3://bucket")
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(123L)
            .config(
                new DeltaStorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    TemporaryCredentials tc = DeltaStorageCredentialUtil.toTemporaryCredentials(c);
    assertThat(tc.getExpirationTime()).isEqualTo(123L);
    assertThat(tc.getAwsTempCredentials().getAccessKeyId()).isEqualTo("ak");
    assertThat(tc.getAwsTempCredentials().getSecretAccessKey()).isEqualTo("sk");
    assertThat(tc.getAwsTempCredentials().getSessionToken()).isEqualTo("st");
  }

  @Test
  void toTemporaryCredentialsRejectsMultiCloudConfig() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("s3://bucket")
            .operation(DeltaCredentialOperation.READ)
            .config(new DeltaStorageCredentialConfig().s3AccessKeyId("ak").gcsOauthToken("gcs"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toTemporaryCredentials(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must contain exactly one cloud credential config");
  }

  @Test
  void toTemporaryCredentialsRejectsMissingConfig() {
    DeltaStorageCredential c =
        new DeltaStorageCredential().prefix("s3://bucket").operation(DeltaCredentialOperation.READ);
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toTemporaryCredentials(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing config");
  }

  @Test
  void toTemporaryCredentialsExtractsAzureSasToken() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("abfss://container@account.dfs.core.windows.net/")
            .operation(DeltaCredentialOperation.READ_WRITE)
            .config(new DeltaStorageCredentialConfig().azureSasToken("sas-token"));
    TemporaryCredentials tc = DeltaStorageCredentialUtil.toTemporaryCredentials(c);
    assertThat(tc.getAzureUserDelegationSas().getSasToken()).isEqualTo("sas-token");
    assertThat(tc.getExpirationTime()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void toTemporaryCredentialsExtractsGcsOauthToken() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("gs://bucket/")
            .operation(DeltaCredentialOperation.READ)
            .expirationTimeMs(456L)
            .config(new DeltaStorageCredentialConfig().gcsOauthToken("gcs-oauth-token"));
    TemporaryCredentials tc = DeltaStorageCredentialUtil.toTemporaryCredentials(c);
    assertThat(tc.getGcpOauthToken().getOauthToken()).isEqualTo("gcs-oauth-token");
    assertThat(tc.getExpirationTime()).isEqualTo(456L);
  }

  @Test
  void toTemporaryCredentialsRejectsPartialS3WithMissingAccessKey() {
    DeltaStorageCredential c =
        new DeltaStorageCredential()
            .prefix("s3://bucket")
            .operation(DeltaCredentialOperation.READ)
            .config(new DeltaStorageCredentialConfig().s3SessionToken("st"));
    assertThatThrownBy(() -> DeltaStorageCredentialUtil.toTemporaryCredentials(c))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("missing S3 access key");
  }

  @Test
  void
      toAdditionalScopedCredentialsEmitsSupportedNonPrimaryScopesAndSkipsUnsupportedDoesNotThrow() {
    String abfssPrefix = "abfss://container@account.dfs.core.windows.net/other";
    DeltaStorageCredential primaryS3 = credAt("s3://bucket/table", Cloud.AWS);
    DeltaStorageCredential gsCred =
        credAt("gs://gbucket/other", Cloud.GCS).operation(DeltaCredentialOperation.READ);
    DeltaStorageCredential abfssCred =
        credAt(abfssPrefix, Cloud.AZURE).operation(DeltaCredentialOperation.READ_WRITE);
    List<DeltaStorageCredential> creds =
        Arrays.asList(
            null,
            new DeltaStorageCredential(),
            credAt("wasbs://c/skip"),
            primaryS3,
            credAt("s3a://b/s3a", Cloud.AWS),
            credAt("file:///local/path"),
            credAt("s3n://b/s3n", Cloud.AWS),
            credAt("/local/path"),
            credAt("s3x://b/s3x", Cloud.AWS),
            gsCred,
            credAt("relative/path"),
            credAt("abfs://container@account.dfs.core.windows.net/abfs", Cloud.AZURE),
            abfssCred);

    // Null elements, null-prefix creds, and config-less unsupported/schemeless entries are all
    // skipped before any URI parse or credential conversion, so the call does not throw.
    assertThatCode(() -> DeltaStorageCredentialUtil.toAdditionalScopedCredentials(primaryS3, creds))
        .doesNotThrowAnyException();

    List<ScopedCredential> scopes =
        DeltaStorageCredentialUtil.toAdditionalScopedCredentials(primaryS3, creds);

    // Scheme matrix (s3*/gs/abfs/abfss accepted, others skipped), the primary dropped by identity,
    // and input order preserved.
    assertThat(scopes)
        .extracting(ScopedCredential::prefix)
        .containsExactly(
            "s3a://b/s3a",
            "s3n://b/s3n",
            "s3x://b/s3x",
            "gs://gbucket/other",
            "abfs://container@account.dfs.core.windows.net/abfs",
            abfssPrefix)
        .doesNotContain(
            "s3://bucket/table",
            "wasbs://c/skip",
            "file:///local/path",
            "/local/path",
            "relative/path");

    // Each emitted scope carries only its own cloud's credential, with the operation wire value.
    // s3a carries no operation, so it also exercises the null -> "READ" default.
    assertScopeCarriesOnly(scopes, "s3a://b/s3a", "READ", Cloud.AWS, "ak");
    assertScopeCarriesOnly(scopes, "gs://gbucket/other", "READ", Cloud.GCS, "gcs-token");
    assertScopeCarriesOnly(scopes, abfssPrefix, "READ_WRITE", Cloud.AZURE, "sas-token");
  }

  private static DeltaStorageCredential credAt(String prefix) {
    return new DeltaStorageCredential().prefix(prefix);
  }

  private static DeltaStorageCredential credAt(String prefix, Cloud cloud) {
    DeltaStorageCredentialConfig config = new DeltaStorageCredentialConfig();
    if (cloud == Cloud.AWS) {
      config.s3AccessKeyId("ak").s3SecretAccessKey("sk").s3SessionToken("st");
    } else if (cloud == Cloud.GCS) {
      config.gcsOauthToken("gcs-token");
    } else {
      config.azureSasToken("sas-token");
    }
    return new DeltaStorageCredential().prefix(prefix).config(config);
  }

  private enum Cloud {
    AWS,
    GCS,
    AZURE
  }

  /**
   * Asserts the scope vended for {@code prefix} has the given {@code operation} and carries ONLY
   * {@code cloud}'s credential — its token equals {@code token} and the other clouds are null.
   */
  private static void assertScopeCarriesOnly(
      List<ScopedCredential> scopes, String prefix, String operation, Cloud cloud, String token) {
    ScopedCredential scope =
        scopes.stream()
            .filter(s -> s.prefix().equals(prefix))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no scope vended for " + prefix));
    assertThat(scope.operation()).isEqualTo(operation);
    TemporaryCredentials creds = scope.credentials();
    if (cloud == Cloud.AWS) {
      assertThat(creds.getAwsTempCredentials().getAccessKeyId()).isEqualTo(token);
    } else {
      assertThat(creds.getAwsTempCredentials()).isNull();
    }
    if (cloud == Cloud.GCS) {
      assertThat(creds.getGcpOauthToken().getOauthToken()).isEqualTo(token);
    } else {
      assertThat(creds.getGcpOauthToken()).isNull();
    }
    if (cloud == Cloud.AZURE) {
      assertThat(creds.getAzureUserDelegationSas().getSasToken()).isEqualTo(token);
    } else {
      assertThat(creds.getAzureUserDelegationSas()).isNull();
    }
  }
}
