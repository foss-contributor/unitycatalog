package io.unitycatalog.hadoop.internal.fs;

import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.StorageLocationUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.DefaultCredId;
import io.unitycatalog.hadoop.internal.id.DelegateFileSystemCacheKey;
import io.unitycatalog.hadoop.internal.util.BoundedKeyedCache;
import io.unitycatalog.hadoop.internal.util.CloseableUtils;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;

/**
 * A Hadoop {@link FileSystem} wrapper that enables multiple credential scopes to coexist within a
 * single Spark session.
 *
 * <h2>Problem</h2>
 *
 * <p>Hadoop's native {@link FileSystem} is designed around a single credential per scheme: its
 * internal cache maps {@code (scheme, authority)} to a shared {@link FileSystem} instance, so all
 * operations on the same URI share the same credential. Unity Catalog, however, vends per-table and
 * per-path temporary credentials, meaning that two different tables backed by the same S3 bucket
 * may require entirely different AWS session tokens at the same time. Simply disabling Hadoop's
 * cache (e.g. {@code fs.s3a.impl.disable.cache=true}) would work functionally but creates a new
 * underlying {@link FileSystem} instance for every file access, quickly exhausting native resources
 * such as S3A connection pools (see <a
 * href="https://github.com/unitycatalog/unitycatalog/issues/1378">issue #1378</a>).
 *
 * <h2>Solution</h2>
 *
 * <p>This class introduces a two-level caching strategy:
 *
 * <ol>
 *   <li><b>Hadoop cache disabled for {@code CredScopedFileSystem} itself.</b> {@link
 *       io.unitycatalog.hadoop.internal.CredPropsUtil} sets {@code
 *       fs.<scheme>.impl.disable.cache=true} so that Hadoop always instantiates a fresh {@code
 *       CredScopedFileSystem} for each file access. Because {@code CredScopedFileSystem} is a thin,
 *       stateless wrapper, this is cheap.
 *   <li><b>Global credential-scoped cache for the real delegate.</b> {@code CredScopedFileSystem}
 *       maintains a static {@link #CACHE} keyed by {@link CredId}, which encodes the credential
 *       scope (table ID + operation, or path + operation). On each {@link #initialize(URI,
 *       Configuration)} call the key is derived from the Hadoop {@link Configuration} injected by
 *       {@link io.unitycatalog.hadoop.internal.CredPropsUtil}, and the corresponding real {@link
 *       FileSystem} (e.g. {@code S3AFileSystem}) is looked up or created. Requests that share the
 *       same credential scope therefore reuse the same underlying connection pool, while requests
 *       with different credentials transparently receive their own isolated instance.
 * </ol>
 *
 * <p>All public {@link FileSystem} operations are delegated to the credential-scoped instance via
 * {@link FilterFileSystem}, so callers see a fully functional filesystem regardless of which
 * underlying implementation backs it.
 */
public class CredScopedFileSystem extends FilterFileSystem {

  private static final String CRED_SCOPED_FS_CACHE_MAX_SIZE =
      "unitycatalog.credScopedFs.cache.maxSize";
  private static final int CRED_SCOPED_FS_CACHE_MAX_SIZE_DEFAULT = 100;

  /**
   * LRU cache of real {@link FileSystem} instances keyed by credential scope. Evicted entries are
   * closed to release connection pools and SDK thread pools (e.g. AWS sdk-ScheduledExecutor
   * threads). The cache is bounded to prevent unbounded growth when many distinct credential scopes
   * are accessed in a long-running session. The maximum size can be tuned via the system property
   * {@code unitycatalog.credScopedFs.cache.maxSize}.
   */
  /** Visible for testing. */
  static final BoundedKeyedCache<DelegateFileSystemCacheKey, FileSystem> CACHE;

  static {
    int maxSize =
        Integer.getInteger(CRED_SCOPED_FS_CACHE_MAX_SIZE, CRED_SCOPED_FS_CACHE_MAX_SIZE_DEFAULT);
    CACHE = new BoundedKeyedCache<>(maxSize, CloseableUtils::closeQuietly);
  }

  /** Visible for testing only. Clears the static cache and closes all cached delegates. */
  static void clearCacheForTesting() {
    CACHE.clear();
  }

  /** Visible for testing only. Returns the cached delegate filesystem. */
  FileSystem getDelegate() {
    return this.fs;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    // One path for every case. The credential to use is selected up front (cheap, read-only) as a
    // namespace: the multi-credential keyspace entry that covers the URI, or null when there is no
    // vended credential (the delegate is keyed by URI scheme+authority instead). Only the cache key
    // and the cheap lookups happen here; the expensive Configuration overlay is deferred into the
    // loader so a cache hit avoids it.
    String namespace = selectNamespace(uri, conf);
    DelegateFileSystemCacheKey key = cacheKey(uri, conf, namespace);
    this.fs = CACHE.getOrLoad(key, () -> newFileSystem(uri, conf, namespace));
  }

  /**
   * Selects the credential keyspace namespace to overlay for {@code uri}. Every UC-vended
   * credential (single or prefix-scoped, fixed or renewable) is serialized into the keyspace, so
   * the driver always writes a count of at least one when the credential-scoped filesystem is
   * enabled:
   *
   * <ul>
   *   <li>{@code count == 0}: no keyspace at all — a non-UC access through an installed {@code
   *       CredScopedFileSystem}. Returns {@code null} so the delegate is keyed by URI alone.
   *   <li>{@code count == 1}: the sole credential covers the request by construction; short-circuit
   *       to namespace 0 without a prefix comparison (its location may be absent, e.g. a legacy UC
   *       table/path credential, which would have nothing to compare against).
   *   <li>{@code count > 1}: select by longest-prefix cover of {@code uri}.
   * </ul>
   *
   * Read-only: performs only string comparisons, never mutates the configuration.
   */
  private static String selectNamespace(URI uri, Configuration conf) {
    int credCount = conf.getInt(UCHadoopConfConstants.UC_SCOPED_CRED_COUNT_KEY, 0);
    if (credCount == 0) {
      // No vended credentials; fall back to a URI-scoped delegate (keyed by scheme+authority).
      return null;
    }
    if (credCount == 1) {
      // The single vended credential always covers the request; no prefix matching needed.
      return UCHadoopConfConstants.UC_SCOPED_CRED_PREFIX + "0.";
    }
    String location = uri.toString();
    List<String> prefixes = new ArrayList<>(credCount);
    for (int i = 0; i < credCount; i++) {
      String namespace = UCHadoopConfConstants.UC_SCOPED_CRED_PREFIX + i + ".";
      prefixes.add(conf.get(namespace + UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY));
    }
    int match = StorageLocationUtil.longestCoveringIndex(location, prefixes);
    Preconditions.checkArgument(
        match >= 0, "No vended credential prefix covers location '%s'.", location);
    return UCHadoopConfConstants.UC_SCOPED_CRED_PREFIX + match + ".";
  }

  /**
   * Builds the delegate-cache key: the request scope ({@link CredId}) paired with the selected
   * credential's location, so different prefixes of the same scope stay isolated. The location
   * comes from the selected {@code namespace} (or the top level when {@code namespace} is null).
   * The CredId is the one the driver wrote at the top level; when none is present (a non-UC access,
   * or a fixed credential which carries no scope keys) it falls back to a URI scheme+authority
   * {@link DefaultCredId} — the same key those accesses used before the keyspace existed.
   */
  private static DelegateFileSystemCacheKey cacheKey(
      URI uri, Configuration conf, String namespace) {
    CredId credId = CredId.create(conf, () -> new DefaultCredId(uri, conf));
    String locationKey =
        (namespace == null ? "" : namespace) + UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY;
    return new DelegateFileSystemCacheKey(credId, conf.get(locationKey));
  }

  /**
   * Restores {@code key} from its {@code key.original} side-channel saved by {@link
   * io.unitycatalog.hadoop.internal.CredPropsUtil}, falling back to {@code defaultImpl} when the
   * side-channel is absent.
   */
  private static void restoreImpl(Configuration fsConf, String key, String defaultImpl) {
    fsConf.set(key, fsConf.get(key + ".original", defaultImpl));
  }

  private static FileSystem newFileSystem(URI uri, Configuration conf) {
    return newFileSystem(uri, conf, null);
  }

  /**
   * Builds the delegate filesystem. When {@code namespace} is non-null (multi-credential path), the
   * selected credential's namespaced keys (its location and init value keys) are overlaid onto the
   * top-level keys; combined with the CredId scope already at the top level, this reconstructs the
   * single-credential conf the driver would have produced. The namespace prefix is stripped so the
   * overlaid keys look exactly like the single-credential case.
   */
  private static FileSystem newFileSystem(URI uri, Configuration conf, String namespace) {
    try {
      Configuration fsConf = new Configuration(conf);

      if (namespace != null) {
        conf.getPropsWithPrefix(namespace).forEach(fsConf::set);
      }

      // S3: restore impl using the side-channel key saved by CredPropsUtil before it overrode
      // fs.<scheme>.impl with CredScopedFileSystem. Falls back to S3AFileSystem if not set.
      restoreImpl(fsConf, "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      restoreImpl(fsConf, "fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      restoreImpl(fsConf, "fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A");
      restoreImpl(fsConf, "fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A");
      fsConf.set("fs.s3.impl.disable.cache", "true");
      fsConf.set("fs.s3a.impl.disable.cache", "true");

      // GCS: restore impl using the side-channel key. Falls back to GoogleHadoopFileSystem if not
      // set (registered via the Java service loader).
      restoreImpl(fsConf, "fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
      restoreImpl(
          fsConf, "fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
      fsConf.set("fs.gs.impl.disable.cache", "true");

      // Azure: restore impl using the side-channel key. Falls back to AzureBlobFileSystem /
      // SecureAzureBlobFileSystem if not set (registered via the Java service loader).
      restoreImpl(fsConf, "fs.abfs.impl", "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem");
      restoreImpl(
          fsConf, "fs.abfss.impl", "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem");
      restoreImpl(fsConf, "fs.AbstractFileSystem.abfs.impl", "org.apache.hadoop.fs.azurebfs.Abfs");
      restoreImpl(
          fsConf, "fs.AbstractFileSystem.abfss.impl", "org.apache.hadoop.fs.azurebfs.Abfss");
      fsConf.set("fs.abfs.impl.disable.cache", "true");
      fsConf.set("fs.abfss.impl.disable.cache", "true");

      return FileSystem.get(uri, fsConf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
