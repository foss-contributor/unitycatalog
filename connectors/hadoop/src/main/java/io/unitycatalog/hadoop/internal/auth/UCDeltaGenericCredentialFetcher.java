package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.delta.api.DeltaTemporaryCredentialsApi;
import io.unitycatalog.client.delta.model.DeltaCredentialOperation;
import io.unitycatalog.client.delta.model.DeltaCredentialsResponse;
import io.unitycatalog.client.delta.model.DeltaStorageCredential;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.DeltaStorageCredentialUtil;
import io.unitycatalog.hadoop.internal.UCDeltaTableIdentifier;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;
import java.util.ArrayList;
import java.util.List;

/** Adapts the UC Delta temporary credentials SDK API for Hadoop token providers. */
final class UCDeltaGenericCredentialFetcher implements GenericCredentialFetcher {
  private final DeltaTemporaryCredentialsApi api;
  private final DeltaTableCredId credId;
  private final DeltaCredentialOperation operation;

  UCDeltaGenericCredentialFetcher(DeltaTableCredId credId, DeltaTemporaryCredentialsApi api) {
    Preconditions.checkNotNull(api, "api is required");
    Preconditions.checkNotNull(credId, "credId is required");
    this.api = api;
    this.credId = credId;
    DeltaCredentialOperation op = DeltaCredentialOperation.fromValue(credId.tableOperation());
    Preconditions.checkArgument(
        op == DeltaCredentialOperation.READ || op == DeltaCredentialOperation.READ_WRITE,
        "UC Delta supports READ and READ_WRITE table operations, got: %s",
        credId.tableOperation());
    this.operation = op;
  }

  @Override
  public List<GenericStorageCredential> createCredentials() throws ApiException {
    List<DeltaStorageCredential> storageCreds = fetchResponse().getStorageCredentials();
    Preconditions.checkArgument(
        storageCreds != null && !storageCreds.isEmpty(),
        "UC Delta API response for table '%s' has no storage credentials.",
        credId.identifier());
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
    UCDeltaTableIdentifier id = credId.identifier();
    DeltaCredentialsResponse response =
        api.getTableCredentials(operation, id.catalog(), id.schema(), id.table());
    Preconditions.checkArgument(
        response != null,
        "UC Delta API returned no credentials response for '%s.%s.%s'.",
        id.catalog(),
        id.schema(),
        id.table());
    return response;
  }
}
