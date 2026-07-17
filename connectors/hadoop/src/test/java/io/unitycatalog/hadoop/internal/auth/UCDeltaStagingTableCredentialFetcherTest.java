package io.unitycatalog.hadoop.internal.auth;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.delta.model.DeltaStorageCredentialConfig;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class UCDeltaStagingTableCredentialFetcherTest {

  private static final UUID STAGING_ID = UUID.randomUUID();
  private static final String LOCATION = "s3://bucket/staging-table";

  @Test
  void createCredentialsCallsDeltaStagingApiAndReturnsCredential() throws Exception {
    DeltaStagingTableCredId credId =
        new DeltaStagingTableCredId(EMPTY_CRED_CONTEXT_ID, STAGING_ID.toString(), LOCATION);
    DeltaCredentialsResponse response = s3StagingResponse();

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(response);

    List<GenericStorageCredential> creds =
        GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredentials();

    assertThat(creds).hasSize(1);
    assertThat(creds.get(0).prefix()).isEqualTo(LOCATION);
    TemporaryCredentials out = creds.get(0).credential().temporaryCredentials();
    assertThat(out.getAwsTempCredentials().getAccessKeyId()).isEqualTo("ak");
    assertThat(out.getAwsTempCredentials().getSecretAccessKey()).isEqualTo("sk");
    assertThat(out.getAwsTempCredentials().getSessionToken()).isEqualTo("st");
    assertThat(out.getExpirationTime()).isEqualTo(1234L);
    verify(api).getStagingTableCredentials(STAGING_ID);
  }

  @Test
  void createCredentialsRejectsNullResponse() throws Exception {
    DeltaStagingTableCredId credId =
        new DeltaStagingTableCredId(EMPTY_CRED_CONTEXT_ID, STAGING_ID.toString(), LOCATION);

    DeltaTemporaryCredentialsApi api = mock(DeltaTemporaryCredentialsApi.class);
    when(api.getStagingTableCredentials(STAGING_ID)).thenReturn(null);

    assertThatThrownBy(
            () -> GenericCredentialFetcher.forUcDeltaStagingTable(credId, api).createCredentials())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("returned no credentials response");
  }

  private static DeltaCredentialsResponse s3StagingResponse() {
    DeltaStorageCredential sc =
        new DeltaStorageCredential()
            .prefix(LOCATION)
            .operation(DeltaCredentialOperation.READ_WRITE)
            .expirationTimeMs(1234L)
            .config(
                new DeltaStorageCredentialConfig()
                    .s3AccessKeyId("ak")
                    .s3SecretAccessKey("sk")
                    .s3SessionToken("st"));
    return new DeltaCredentialsResponse().addStorageCredentialsItem(sc);
  }
}
