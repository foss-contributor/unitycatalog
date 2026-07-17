package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Adapts the UC Delta staging table credentials SDK API for Hadoop token providers. */
final class UCDeltaStagingTableCredentialFetcher implements GenericCredentialFetcher {

  private final DeltaTemporaryCredentialsApi api;
  private final UUID stagingTableId;

  UCDeltaStagingTableCredentialFetcher(
      DeltaStagingTableCredId credId, DeltaTemporaryCredentialsApi api) {
    Preconditions.checkNotNull(credId, "credId is required");
    Preconditions.checkNotNull(api, "Temporary credentials API is required");

    this.api = api;
    this.stagingTableId = UUID.fromString(credId.stagingTableId());
  }

  @Override
  public List<GenericStorageCredential> createCredentials() throws ApiException {
    List<DeltaStorageCredential> storageCreds = fetchResponse().getStorageCredentials();
    Preconditions.checkArgument(
        storageCreds != null && !storageCreds.isEmpty(),
        "UC Delta API response for staging table '%s' has no storage credentials.",
        stagingTableId);
    List<GenericStorageCredential> out = new ArrayList<>(storageCreds.size());
    for (DeltaStorageCredential storageCred : storageCreds) {
      out.add(
          new GenericStorageCredential(
              new GenericCredential(DeltaStorageCredentialUtil.toTemporaryCredentials(storageCred)),
              storageCred.getPrefix()));
    }
    return out;
  }

  private DeltaCredentialsResponse fetchResponse() throws ApiException {
    DeltaCredentialsResponse response = api.getStagingTableCredentials(stagingTableId);
    Preconditions.checkNotNull(
        response,
        "UC Delta API returned no credentials response for staging table '%s'.",
        stagingTableId);
    return response;
  }
}
