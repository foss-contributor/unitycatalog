package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;
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
import io.unitycatalog.hadoop.internal.auth.ScopedCredential;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end wire contract between the producer (CredPropsUtil) and the consumer
 * (CredentialScopes.selectConf and CredScopedKey.create): properties emitted for an additional
 * location decode back into that location's credentials and yield a distinct cache key from the
 * table's own.
 */
class CredScopeWireContractTest {

  @AfterEach
  void resetFactory() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
  }

  @Test
  void emittedScopePropsDecodeBackToTheScopeLocationAndDistinctKey() throws Exception {
    String scopePrefix = "s3://secondary/data";
    GenericCredentialFetcher fetcher = mock(GenericCredentialFetcher.class);
    when(fetcher.createCredential()).thenReturn(new GenericCredential(s3Creds()));
    when(fetcher.additionalScopedCredentials())
        .thenReturn(List.of(new ScopedCredential(scopePrefix, "READ", s3Creds())));
    CredPropsUtil.genericCredFetcherFactory = (apiClient, conf) -> fetcher;

    // Producer: real properties for a table whose reads span its own location plus one scope.
    Map<String, String> props =
        CredPropsUtil.fetchDeltaTableCredProps(
            /* renew= */ true,
            /* credScopedFsEnabled= */ true,
            new Configuration(false),
            "s3",
            null,
            "http://uc",
            tokenProvider(),
            UCDeltaTableIdentifier.of("cat", "sch", "tbl"),
            "s3://own/tbl",
            UCCredentialHadoopConfs.TableOperation.READ_WRITE,
            Map.of());
    Configuration conf = new Configuration(false);
    props.forEach(conf::set);

    // Consumer: a path under the scope selects the scope's overlaid location and operation.
    URI coveredUri = URI.create(scopePrefix + "/part-0.parquet");
    Configuration covered = CredentialScopes.selectConf(coveredUri, conf);
    assertThat(covered).isNotSameAs(conf);
    assertThat(covered.get(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY)).isEqualTo(scopePrefix);
    assertThat(covered.get(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY)).isEqualTo("READ");

    // A path under the table's own location is not covered (conf returned unchanged).
    URI ownUri = URI.create("s3://own/tbl/data/x.parquet");
    assertThat(CredentialScopes.selectConf(ownUri, conf)).isSameAs(conf);

    // The overlaid scope conf yields a distinct filesystem cache key from the table's own.
    assertThat(CredScopedKey.create(coveredUri, covered))
        .isNotEqualTo(CredScopedKey.create(ownUri, conf));
  }

  private static TokenProvider tokenProvider() {
    return TokenProvider.create(Map.of("type", "static", "token", "tok"));
  }

  private static TemporaryCredentials s3Creds() {
    return new TemporaryCredentials()
        .awsTempCredentials(
            new AwsCredentials().accessKeyId("ak").secretAccessKey("sk").sessionToken("st"));
  }
}
