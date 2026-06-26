package io.unitycatalog.hadoop.internal.fs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class CredentialScopesTest {

  @Test
  void selectConfReturnsInputByIdentityWhenNoScopesOrUriNull() {
    URI uri = URI.create("s3://bucket/data");

    // No scopes configured (count key absent) -> no-op; returns the same conf (backward compat).
    Configuration noScopes = new Configuration(false);
    assertThat(CredentialScopes.selectConf(uri, noScopes)).isSameAs(noScopes);

    // A null uri short-circuits before uri.toString(), even with count > 0 (no NPE).
    Configuration withCount = new Configuration(false);
    withCount.set(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, "1");
    assertThat(CredentialScopes.selectConf(null, withCount)).isSameAs(withCount);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("selectionCases")
  void selectConfOverlaysCoveringScopeOntoCopyWithoutMutatingInput(
      String uri, String expectedKey, String expectedValue) {
    Configuration conf = confWithScopes();

    Configuration out = CredentialScopes.selectConf(URI.create(uri), conf);

    if (expectedKey == null) {
      // Not covered (boundary near-miss / different bucket) -> no-op, same conf.
      assertThat(out).isSameAs(conf);
    } else {
      // Covered -> overlaid onto a copy; the winning scope's value surfaces; input not mutated.
      assertThat(out).isNotSameAs(conf);
      assertThat(out.get(expectedKey)).isEqualTo(expectedValue);
      assertThat(conf.get(expectedKey)).as("input conf must not be mutated").isNull();
    }
  }

  private static Stream<Arguments> selectionCases() {
    return Stream.of(
        // Longest covering prefix wins (foo vs foo/nested).
        arguments("s3a://b/foo/file", "fs.s3a.access.key", "fooKey"),
        arguments("s3a://b/foo/nested/file", "fs.s3a.access.key", "nestedKey"),
        // Segment-boundary near-miss: foobar is not covered by foo.
        arguments("s3a://b/foobar/file", null, null),
        // Cloud-agnostic routing: gs and abfss paths select their own scopes.
        arguments("gs://g/data/file", "fs.gs.auth.access.token", "gsKey"),
        arguments("abfss://c@a/data/file", "fs.azure.sas.fixed.token", "azKey"),
        // Different bucket -> not covered.
        arguments("s3a://b/other/file", null, null));
  }

  @Test
  void selectConfPicksLongestPrefixRegardlessOfScopeOrderAndTiesToFirstIndex() {
    // Inner listed before outer -> longest still wins (by length, not index order).
    Map<String, String> swapped = new HashMap<>();
    scope(swapped, 0, "s3a://b/foo/nested", "fs.s3a.access.key", "nestedKey");
    scope(swapped, 1, "s3a://b/foo", "fs.s3a.access.key", "fooKey");
    Configuration out =
        CredentialScopes.selectConf(URI.create("s3a://b/foo/nested/x"), scopedConf(swapped, 2));
    assertThat(out.get("fs.s3a.access.key")).isEqualTo("nestedKey");

    // Equal-length tie -> first index wins (the selection loop uses strict >).
    Map<String, String> tie = new HashMap<>();
    scope(tie, 0, "s3a://b/foo", "fs.s3a.access.key", "firstKey");
    scope(tie, 1, "s3a://b/foo", "fs.s3a.access.key", "secondKey");
    Configuration tieOut =
        CredentialScopes.selectConf(URI.create("s3a://b/foo/x"), scopedConf(tie, 2));
    assertThat(tieOut.get("fs.s3a.access.key")).isEqualTo("firstKey");
  }

  @Test
  void selectConfOverlaysOnlyWinningScopeNamespaceSoLoserKeysDoNotLeak() {
    // Two scopes cover the same path; the longer (inner) wins. The shorter (outer) carries a
    // distinct key the winner does not -> it must not leak into the result.
    Map<String, String> encoded = new HashMap<>();
    scope(encoded, 0, "s3a://b/foo", "fs.s3a.outer.only.key", "leak");
    scope(encoded, 1, "s3a://b/foo/nested", "fs.s3a.access.key", "innerKey");

    Configuration out =
        CredentialScopes.selectConf(URI.create("s3a://b/foo/nested/file"), scopedConf(encoded, 2));
    assertThat(out.get("fs.s3a.access.key")).isEqualTo("innerKey");
    assertThat(out.get("fs.s3a.outer.only.key")).isNull();
  }

  @Test
  void encodeWritesPrefixKeyAndNamespacedPropsWithIndexIsolation() {
    Map<String, String> into = new HashMap<>();
    CredentialScopes.encode(
        into, 2, "s3://b/x", Map.of("fs.s3a.access.key", "AK", "fs.s3a.secret.key", "SK"));

    // One scope -> its prefix key plus one namespaced entry per prop (3 total).
    assertThat(into).hasSize(3);
    assertThat(into).containsEntry(prefixKey(2), "s3://b/x");
    assertThat(into).containsEntry(propKey(2, "fs.s3a.access.key"), "AK");
    assertThat(into).containsEntry(propKey(2, "fs.s3a.secret.key"), "SK");

    // A second index writes a disjoint namespace and leaves index 2 untouched.
    CredentialScopes.encode(into, 5, "s3://b/y", Map.of("fs.s3a.access.key", "AK5"));
    assertThat(into).containsEntry(prefixKey(5), "s3://b/y");
    assertThat(into).containsEntry(propKey(5, "fs.s3a.access.key"), "AK5");
    assertThat(into).containsEntry(prefixKey(2), "s3://b/x");
    assertThat(into).containsEntry(propKey(2, "fs.s3a.access.key"), "AK");
  }

  private static Configuration confWithScopes() {
    Map<String, String> encoded = new HashMap<>();
    scope(encoded, 0, "s3a://b/foo", "fs.s3a.access.key", "fooKey");
    scope(encoded, 1, "s3a://b/foo/nested", "fs.s3a.access.key", "nestedKey");
    scope(encoded, 2, "gs://g/data", "fs.gs.auth.access.token", "gsKey");
    scope(encoded, 3, "abfss://c@a/data", "fs.azure.sas.fixed.token", "azKey");
    return scopedConf(encoded, 4);
  }

  private static void scope(
      Map<String, String> into, int index, String prefix, String key, String value) {
    CredentialScopes.encode(into, index, prefix, Map.of(key, value));
  }

  private static Configuration scopedConf(Map<String, String> encoded, int count) {
    Configuration conf = new Configuration(false);
    encoded.forEach(conf::set);
    conf.set(UCHadoopConfConstants.UC_CRED_SCOPE_COUNT_KEY, String.valueOf(count));
    return conf;
  }

  private static String prefixKey(int index) {
    return UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX
        + index
        + UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX_SUFFIX;
  }

  private static String propKey(int index, String key) {
    return UCHadoopConfConstants.UC_CRED_SCOPE_PREFIX
        + index
        + UCHadoopConfConstants.UC_CRED_SCOPE_PROP_SUFFIX
        + key;
  }
}
