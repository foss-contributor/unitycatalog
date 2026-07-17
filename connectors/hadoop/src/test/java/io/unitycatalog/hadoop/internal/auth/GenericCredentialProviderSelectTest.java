package io.unitycatalog.hadoop.internal.auth;

import static io.unitycatalog.hadoop.internal.id.CredIdTest.EMPTY_CRED_CONTEXT_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that {@link GenericCredentialProvider}, on renewal, selects the vended credential whose
 * prefix covers the request location (the executor side of the multi-credential flow).
 */
class GenericCredentialProviderSelectTest {

  @AfterEach
  void clearGlobalCache() {
    GenericCredentialProvider.globalCache.clear();
  }

  /**
   * Delta-table conf whose {@code selectionLocation} (top-level {@code UC_CREDENTIAL_LOCATION_KEY})
   * drives which vended credential is chosen at renewal. The CredId's own location is fixed.
   */
  private static Configuration deltaConf(String selectionLocation) {
    Configuration conf = new Configuration();
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
    conf.set(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, selectionLocation);
    return conf;
  }

  private static GenericStorageCredential awsAt(String prefix, String id) {
    TemporaryCredentials tc =
        new TemporaryCredentials()
            .awsTempCredentials(new AwsCredentials().accessKeyId("ak" + id))
            .expirationTime(Long.MAX_VALUE);
    return new GenericStorageCredential(new GenericCredential(tc), prefix);
  }

  /** A provider whose fetcher returns a fixed multi-credential batch. */
  private static class TestProvider extends GenericCredentialProvider {
    private final GenericCredentialFetcher fetcher;

    TestProvider(Configuration conf, GenericCredentialFetcher fetcher) {
      this.fetcher = fetcher;
      initialize(conf);
    }

    @Override
    public GenericCredential initGenericCredential(Configuration conf) {
      return null; // force a renewal on first access
    }

    @Override
    GenericCredentialFetcher genericCredentialFetcher() {
      return fetcher;
    }
  }

  private static GenericCredentialFetcher fetcherReturning(GenericStorageCredential... creds)
      throws Exception {
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    when(fetcher.createCredentials()).thenReturn(Arrays.asList(creds));
    return fetcher;
  }

  @Test
  void selectsCredentialWhosePrefixCoversLocation() throws Exception {
    GenericCredentialFetcher fetcher =
        fetcherReturning(awsAt("s3://bucket/a", "-a"), awsAt("s3://bucket/b", "-b"));
    TestProvider provider = new TestProvider(deltaConf("s3://bucket/b"), fetcher);

    GenericCredential selected = provider.accessCredentials();

    assertThat(selected.temporaryCredentials().getAwsTempCredentials().getAccessKeyId())
        .isEqualTo("ak-b");
  }

  @Test
  void throwsWhenNoPrefixCoversLocation() throws Exception {
    GenericCredentialFetcher fetcher =
        fetcherReturning(awsAt("s3://bucket/a", "-a"), awsAt("s3://bucket/b", "-b"));
    TestProvider provider = new TestProvider(deltaConf("s3://bucket/c"), fetcher);

    assertThatThrownBy(provider::accessCredentials)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("No vended credential prefix covers location");
  }
}
