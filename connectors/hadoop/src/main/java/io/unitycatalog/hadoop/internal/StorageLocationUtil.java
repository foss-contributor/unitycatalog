package io.unitycatalog.hadoop.internal;

import java.util.List;
import java.util.Locale;

/** Utilities for matching storage locations against credential prefixes. */
public final class StorageLocationUtil {
  private StorageLocationUtil() {}

  /**
   * Returns the index of the {@code prefixes} entry that covers {@code location} with the longest
   * prefix, or {@code -1} if none covers it. Null/empty prefixes are ignored. Used to select among
   * several prefix-scoped credentials serialized into the multi-credential keyspace.
   */
  public static int longestCoveringIndex(String location, List<String> prefixes) {
    int best = -1;
    int bestLen = -1;
    for (int i = 0; i < prefixes.size(); i++) {
      String prefix = prefixes.get(i);
      if (prefix == null || prefix.isEmpty() || !prefixCovers(location, prefix)) {
        continue;
      }
      int len = stripTrailingSlashes(prefix).length();
      if (len > bestLen) {
        best = i;
        bestLen = len;
      }
    }
    return best;
  }

  static boolean prefixCovers(String location, String prefix) {
    String l = normalizeScheme(stripTrailingSlashes(location));
    String p = normalizeScheme(stripTrailingSlashes(prefix));
    return !p.isEmpty() && (l.equals(p) || (l.startsWith(p) && l.charAt(p.length()) == '/'));
  }

  /**
   * Collapses object-store scheme aliases to a canonical family so a request URI and a vended
   * prefix that denote the same storage location still match. Normalizes the scheme and authority
   * only.
   */
  private static String normalizeScheme(String value) {
    int sep = value.indexOf("://");
    if (sep < 0) {
      return value;
    }
    String scheme = value.substring(0, sep).toLowerCase(Locale.ROOT);
    String rest = value.substring(sep); // includes "://"
    switch (scheme) {
      case "s3":
      case "s3a":
      case "s3n":
        return "s3" + rest;
      case "abfs":
      case "abfss":
        return "abfs" + rest;
      default:
        return scheme + rest;
    }
  }

  private static String stripTrailingSlashes(String value) {
    int end = value.length();
    int min = value.indexOf("://");
    min = min >= 0 ? min + 3 : 1;
    while (end > min && value.charAt(end - 1) == '/') {
      end--;
    }
    return value.substring(0, end);
  }
}
