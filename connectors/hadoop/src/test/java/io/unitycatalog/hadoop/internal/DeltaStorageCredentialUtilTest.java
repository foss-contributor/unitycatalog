package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.model.TemporaryCredentials;
import org.junit.jupiter.api.Test;

class DeltaStorageCredentialUtilTest {

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
}
