package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

class CredentialScopesTest {

  /** Three scopes: two sharing a bucket (one nested under the other) and one in another bucket. */
  private static Configuration confWithScopes() {
    Map<String, String> props = new HashMap<>();
    CredentialScopes.encode(
        props, 0, "s3://bucket/tables/loc1", Map.of("fs.s3a.access.key", "loc1Key"));
    CredentialScopes.encode(
        props, 1, "s3://bucket/tables/loc1/nested", Map.of("fs.s3a.access.key", "nestedKey"));
    CredentialScopes.encode(
        props, 2, "s3://otherbucket/tables/loc2", Map.of("fs.s3a.access.key", "loc2Key"));
    props.put(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, "3");
    Configuration conf = new Configuration(false);
    conf.set("fs.s3a.access.key", "primaryKey");
    props.forEach(conf::set);
    return conf;
  }

  @Test
  void selectConfOverlaysTheCoveringScopeIncludingSameBucketScopes() {
    Configuration conf = confWithScopes();
    Configuration sameBucket =
        CredentialScopes.selectConf(URI.create("s3://bucket/tables/loc1/part-0.parquet"), conf);
    assertThat(sameBucket.get("fs.s3a.access.key")).isEqualTo("loc1Key");
    Configuration crossBucket =
        CredentialScopes.selectConf(URI.create("s3://otherbucket/tables/loc2/f"), conf);
    assertThat(crossBucket.get("fs.s3a.access.key")).isEqualTo("loc2Key");
  }

  @Test
  void selectConfPicksTheLongestCoveringPrefix() {
    Configuration conf = confWithScopes();
    Configuration selected =
        CredentialScopes.selectConf(
            URI.create("s3://bucket/tables/loc1/nested/part-0.parquet"), conf);
    assertThat(selected.get("fs.s3a.access.key")).isEqualTo("nestedKey");
  }

  @Test
  void selectConfReturnsSameConfWhenNoScopeCovers() {
    Configuration conf = confWithScopes();
    assertThat(CredentialScopes.selectConf(URI.create("s3://bucket/tables/uncovered/f"), conf))
        .isSameAs(conf);
    // Coverage is per path segment, not raw string prefix.
    assertThat(CredentialScopes.selectConf(URI.create("s3://bucket/tables/loc10/f"), conf))
        .isSameAs(conf);
  }

  @Test
  void selectConfReturnsSameConfWithoutScopes() {
    Configuration conf = new Configuration(false);
    assertThat(CredentialScopes.selectConf(URI.create("s3://bucket/x"), conf)).isSameAs(conf);
    assertThat(CredentialScopes.selectConf(null, conf)).isSameAs(conf);
  }

  @Test
  void selectConfOverlaysAnyCloudScopeByPrefix() {
    // Selection is pure prefix matching and the overlay copies arbitrary keys, so an S3, GCS,
    // and Azure scope coexist in one conf and each path selects its own cloud's credential.
    Map<String, String> props = new HashMap<>();
    CredentialScopes.encode(props, 0, "s3://bucket/loc-s3", Map.of("fs.s3a.access.key", "s3Key"));
    CredentialScopes.encode(
        props, 1, "gs://bucket/loc-gs", Map.of("fs.gs.auth.access.token.credential", "gsTok"));
    CredentialScopes.encode(
        props,
        2,
        "abfss://container@account.dfs.core.windows.net/loc-abfs",
        Map.of("fs.azure.sas.fixed.token", "abfsSas"));
    props.put(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, "3");
    Configuration conf = new Configuration(false);
    props.forEach(conf::set);

    assertThat(
            CredentialScopes.selectConf(URI.create("s3://bucket/loc-s3/f"), conf)
                .get("fs.s3a.access.key"))
        .isEqualTo("s3Key");
    assertThat(
            CredentialScopes.selectConf(URI.create("gs://bucket/loc-gs/f"), conf)
                .get("fs.gs.auth.access.token.credential"))
        .isEqualTo("gsTok");
    assertThat(
            CredentialScopes.selectConf(
                    URI.create("abfss://container@account.dfs.core.windows.net/loc-abfs/f"), conf)
                .get("fs.azure.sas.fixed.token"))
        .isEqualTo("abfsSas");
  }

  @Test
  void overlaidScopeYieldsADistinctCredScopedKey() {
    // A path under the table's own location and a path under an additional scope resolve to
    // different credential scopes, hence different delegate filesystems.
    Configuration conf = new Configuration(false);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true");
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "tbl");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/tables/main");
    Map<String, String> scopeProps = new HashMap<>();
    CredentialScopes.encode(
        scopeProps,
        0,
        "s3://bucket/tables/other",
        Map.of(
            UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/tables/other",
            UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ"));
    scopeProps.forEach(conf::set);
    conf.set(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, "1");

    URI ownFile = URI.create("s3://bucket/tables/main/part-0.parquet");
    URI scopeFile = URI.create("s3://bucket/tables/other/part-0.parquet");
    CredScopedKey ownKey =
        CredScopedKey.create(ownFile, CredentialScopes.selectConf(ownFile, conf));
    CredScopedKey scopeKey =
        CredScopedKey.create(scopeFile, CredentialScopes.selectConf(scopeFile, conf));
    assertThat(scopeKey).isNotEqualTo(ownKey);
  }
}
