package io.unitycatalog.hadoop.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link StorageLocationUtil}: prefix-cover matching and scheme-alias normalization used
 * to select which vended credential applies to a requested storage URI.
 */
class StorageLocationUtilTest {

  @Test
  void prefixCoversMatchesAtPathBoundary() {
    assertThat(StorageLocationUtil.prefixCovers("s3://bucket/t", "s3://bucket/t")).isTrue();
    assertThat(StorageLocationUtil.prefixCovers("s3://bucket/t/x", "s3://bucket/t")).isTrue();
    assertThat(StorageLocationUtil.prefixCovers("s3://bucket/t-other", "s3://bucket/t")).isFalse();
  }

  @Test
  void prefixCoversNormalizesTrailingSlashes() {
    assertThat(StorageLocationUtil.prefixCovers("s3://bucket/t//", "s3://bucket/t")).isTrue();
    assertThat(StorageLocationUtil.prefixCovers("s3://bucket/t", "s3://bucket/t///")).isTrue();
  }

  @Test
  void longestCoveringIndexPicksLongestCoveringPrefix() {
    List<String> prefixes = Arrays.asList("s3://bucket", "s3://bucket/t", "s3://bucket/t/child");
    assertThat(StorageLocationUtil.longestCoveringIndex("s3://bucket/t/child/file", prefixes))
        .isEqualTo(2);
    assertThat(StorageLocationUtil.longestCoveringIndex("s3://bucket/t/file", prefixes))
        .isEqualTo(1);
  }

  @Test
  void longestCoveringIndexSkipsNullAndEmptyPrefixes() {
    List<String> prefixes = Arrays.asList(null, "", "s3://bucket/t");
    assertThat(StorageLocationUtil.longestCoveringIndex("s3://bucket/t/file", prefixes))
        .isEqualTo(2);
  }

  @Test
  void longestCoveringIndexReturnsMinusOneWhenNoneCovers() {
    List<String> prefixes = Arrays.asList("s3://other", "s3://bucket/sibling");
    assertThat(StorageLocationUtil.longestCoveringIndex("s3://bucket/t", prefixes)).isEqualTo(-1);
  }

  @Test
  void s3aRequestMatchesS3VendedPrefix() {
    // The UC server vends the prefix as s3://; Hadoop's S3A filesystem accesses it as s3a://.
    // Scheme aliases must be treated as the same store.
    assertThat(
            StorageLocationUtil.longestCoveringIndex(
                "s3a://bucket/tables/uuid/part-0.parquet",
                Arrays.asList("s3://bucket/tables/uuid")))
        .isEqualTo(0);
  }

  @Test
  void s3RequestMatchesS3aVendedPrefix() {
    // Symmetric: prefix vended as s3a://, request via s3://.
    assertThat(
            StorageLocationUtil.longestCoveringIndex(
                "s3://bucket/t/file", Arrays.asList("s3a://bucket/t")))
        .isEqualTo(0);
  }

  @Test
  void s3nAliasMatchesS3() {
    assertThat(
            StorageLocationUtil.longestCoveringIndex(
                "s3n://bucket/t/file", Arrays.asList("s3://bucket/t")))
        .isEqualTo(0);
  }

  @Test
  void abfssRequestMatchesAbfsVendedPrefix() {
    assertThat(
            StorageLocationUtil.longestCoveringIndex(
                "abfss://c@a.dfs.core.windows.net/t/file",
                Arrays.asList("abfs://c@a.dfs.core.windows.net/t")))
        .isEqualTo(0);
  }

  @Test
  void longestPrefixStillWinsAcrossSchemeAliases() {
    // Aliasing must not break longest-match selection: the deeper prefix wins even when the
    // request scheme differs from the vended schemes.
    List<String> prefixes = Arrays.asList("s3://bucket", "s3a://bucket/t", "s3n://bucket/t/child");
    assertThat(StorageLocationUtil.longestCoveringIndex("s3a://bucket/t/child/file", prefixes))
        .isEqualTo(2);
  }

  @Test
  void differentSchemeFamiliesDoNotMatch() {
    // gs and s3 are distinct stores and must never alias to each other.
    assertThat(
            StorageLocationUtil.longestCoveringIndex(
                "gs://bucket/t/file", Arrays.asList("s3://bucket/t")))
        .isEqualTo(-1);
  }

  @Test
  void differentBucketDoesNotMatchDespiteSchemeAlias() {
    // Aliasing the scheme must not loosen the authority/path check.
    assertThat(
            StorageLocationUtil.longestCoveringIndex(
                "s3a://other/t/file", Arrays.asList("s3://bucket/t")))
        .isEqualTo(-1);
  }
}
