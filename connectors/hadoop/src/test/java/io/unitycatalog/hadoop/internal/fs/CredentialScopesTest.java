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
        props, 0, "s3://bucket/tables/base1", Map.of("fs.s3a.access.key", "base1Key"));
    CredentialScopes.encode(
        props, 1, "s3://bucket/tables/base1/nested", Map.of("fs.s3a.access.key", "nestedKey"));
    CredentialScopes.encode(
        props, 2, "s3://otherbucket/tables/base2", Map.of("fs.s3a.access.key", "base2Key"));
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
        CredentialScopes.selectConf(URI.create("s3://bucket/tables/base1/part-0.parquet"), conf);
    assertThat(sameBucket.get("fs.s3a.access.key")).isEqualTo("base1Key");
    Configuration crossBucket =
        CredentialScopes.selectConf(URI.create("s3://otherbucket/tables/base2/f"), conf);
    assertThat(crossBucket.get("fs.s3a.access.key")).isEqualTo("base2Key");
  }

  @Test
  void selectConfPicksTheLongestCoveringPrefix() {
    Configuration conf = confWithScopes();
    Configuration selected =
        CredentialScopes.selectConf(
            URI.create("s3://bucket/tables/base1/nested/part-0.parquet"), conf);
    assertThat(selected.get("fs.s3a.access.key")).isEqualTo("nestedKey");
  }

  @Test
  void selectConfReturnsSameConfWhenNoScopeCovers() {
    Configuration conf = confWithScopes();
    assertThat(CredentialScopes.selectConf(URI.create("s3://bucket/tables/clone/f"), conf))
        .isSameAs(conf);
    // Coverage is per path segment, not raw string prefix.
    assertThat(CredentialScopes.selectConf(URI.create("s3://bucket/tables/base10/f"), conf))
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
    CredentialScopes.encode(props, 0, "s3://bucket/s3base", Map.of("fs.s3a.access.key", "s3Key"));
    CredentialScopes.encode(
        props, 1, "gs://bucket/gsbase", Map.of("fs.gs.auth.access.token.credential", "gsTok"));
    CredentialScopes.encode(
        props,
        2,
        "abfss://container@account.dfs.core.windows.net/abfsbase",
        Map.of("fs.azure.sas.fixed.token", "abfsSas"));
    props.put(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, "3");
    Configuration conf = new Configuration(false);
    props.forEach(conf::set);

    assertThat(
            CredentialScopes.selectConf(URI.create("s3://bucket/s3base/f"), conf)
                .get("fs.s3a.access.key"))
        .isEqualTo("s3Key");
    assertThat(
            CredentialScopes.selectConf(URI.create("gs://bucket/gsbase/f"), conf)
                .get("fs.gs.auth.access.token.credential"))
        .isEqualTo("gsTok");
    assertThat(
            CredentialScopes.selectConf(
                    URI.create("abfss://container@account.dfs.core.windows.net/abfsbase/f"), conf)
                .get("fs.azure.sas.fixed.token"))
        .isEqualTo("abfsSas");
  }

  @Test
  void overlaidScopeYieldsADistinctCredScopedKey() {
    Configuration conf = new Configuration(false);
    conf.set(
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_KEY,
        UCHadoopConfConstants.UC_CREDENTIALS_TYPE_TABLE_VALUE);
    conf.set(UCHadoopConfConstants.UC_DELTA_CREDENTIALS_API_ENABLED_KEY, "true");
    conf.set(UCHadoopConfConstants.UC_DELTA_CATALOG_KEY, "cat");
    conf.set(UCHadoopConfConstants.UC_DELTA_SCHEMA_KEY, "sch");
    conf.set(UCHadoopConfConstants.UC_DELTA_TABLE_NAME_KEY, "clone");
    conf.set(UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ_WRITE");
    conf.set(UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/tables/clone");
    Map<String, String> scopeProps = new HashMap<>();
    CredentialScopes.encode(
        scopeProps,
        0,
        "s3://bucket/tables/base",
        Map.of(
            UCHadoopConfConstants.UC_DELTA_LOCATION_KEY, "s3://bucket/tables/base",
            UCHadoopConfConstants.UC_TABLE_OPERATION_KEY, "READ"));
    scopeProps.forEach(conf::set);
    conf.set(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, "1");

    URI cloneFile = URI.create("s3://bucket/tables/clone/part-0.parquet");
    URI baseFile = URI.create("s3://bucket/tables/base/part-0.parquet");
    CredScopedKey cloneKey =
        CredScopedKey.create(cloneFile, CredentialScopes.selectConf(cloneFile, conf));
    CredScopedKey baseKey =
        CredScopedKey.create(baseFile, CredentialScopes.selectConf(baseFile, conf));
    assertThat(baseKey).isNotEqualTo(cloneKey);
  }
}
