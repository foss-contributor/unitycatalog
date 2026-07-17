package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.DelegateFileSystemCacheKey;
import io.unitycatalog.hadoop.internal.util.BoundedKeyedCache;
import java.util.ArrayList;
import java.util.List;

/**
 * Caches vended credentials of type {@code T} keyed by {@code K}. A cached value is reused while it
 * is still valid and re-fetched via the supplied factory only once it is about to expire.
 *
 * <p>The cache is agnostic to the key and value types: the executor-side global cache keys a single
 * {@link GenericCredential} by {@link DelegateFileSystemCacheKey}, while the driver-side initial
 * cache keys a {@code List<GenericStorageCredential>} by {@link CredId}. The caller supplies a
 * {@link Staleness} predicate describing when a value is ready to renew.
 */
public class CredentialCache<K, T> {
  private static final int DEFAULT_MAX_SIZE = 1024;
  private static final String GLOBAL_CACHE_MAX_SIZE_KEY = "unitycatalog.credential.cache.maxSize";
  private static final String INITIAL_CACHE_MAX_SIZE_KEY =
      "unitycatalog.initial.credential.cache.maxSize";

  private final BoundedKeyedCache<K, RenewableCredential<T>> cache;

  public CredentialCache(int maxSize) {
    this.cache = new BoundedKeyedCache<>(maxSize);
  }

  /**
   * Creates the JVM-wide cache used by the Hadoop token providers to renew and share vended
   * credentials across requests targeting the same scope, saving QPS to the Unity Catalog server.
   * Keyed by {@link DelegateFileSystemCacheKey} so that credentials at different prefixes of the
   * same scope stay isolated.
   */
  public static CredentialCache<DelegateFileSystemCacheKey, GenericCredential> createGlobalCache() {
    return new CredentialCache<>(Integer.getInteger(GLOBAL_CACHE_MAX_SIZE_KEY, DEFAULT_MAX_SIZE));
  }

  /**
   * Creates the cache for the initial credentials fetched during query planning (e.g. on the Spark
   * driver) so that different queries targeting the same scope reuse the same vended credentials
   * instead of re-fetching from the Unity Catalog server.
   */
  public static CredentialCache<CredId, List<GenericStorageCredential>>
      createInitialCredentialCache() {
    return new CredentialCache<>(Integer.getInteger(INITIAL_CACHE_MAX_SIZE_KEY, DEFAULT_MAX_SIZE));
  }

  /**
   * Returns the value for {@code key}, handling three cases:
   *
   * <ul>
   *   <li>Cached and still valid: return it as is.
   *   <li>Cached but about to expire: create a fresh one via {@code factory}, cache it, return it.
   *   <li>Not cached: create it via {@code factory}, cache it, return it.
   * </ul>
   */
  public T access(K key, RenewableCredentialFactory<T> factory) throws ApiException {
    synchronized (cache) {
      RenewableCredential<T> cached = cache.getIfPresent(key);
      // Reuse the cached value while it's still valid; otherwise fetch and cache a fresh one.
      if (cached != null && !cached.readyToRenew()) {
        return cached.credential();
      }

      RenewableCredential<T> created = factory.create();
      cache.put(key, created);
      return created.credential();
    }
  }

  /** Removes all cached values. Public so tests in other packages can reset shared caches. */
  public void clear() {
    cache.clear();
  }

  // Visible for testing only.
  int size() {
    return cache.size();
  }

  // Visible for testing only.
  List<T> credentials() {
    List<T> values = new ArrayList<>();
    for (RenewableCredential<T> entry : cache.values()) {
      values.add(entry.credential());
    }
    return values;
  }

  @FunctionalInterface
  public interface RenewableCredentialFactory<T> {
    RenewableCredential<T> create() throws ApiException;
  }

  /** Decides whether a cached value is ready to be renewed for the given clock and lead time. */
  @FunctionalInterface
  public interface Staleness<T> {
    boolean readyToRenew(T value, Clock clock, long renewalLeadTimeMillis);
  }

  /**
   * A cached value together with the renewal policy ({@code clock} and {@code
   * renewalLeadTimeMillis}) and the {@link Staleness} predicate used to decide when it should be
   * renewed. The policy is captured from the caller that created the entry; since both are derived
   * from the same Hadoop configuration, later readers observe the same renewal behavior.
   */
  public static class RenewableCredential<T> {
    private final long renewalLeadTimeMillis;
    private final Clock clock;
    private final T credential;
    private final Staleness<T> staleness;

    public RenewableCredential(
        long renewalLeadTimeMillis, Clock clock, T credential, Staleness<T> staleness) {
      this.renewalLeadTimeMillis = renewalLeadTimeMillis;
      this.clock = clock;
      this.credential = credential;
      this.staleness = staleness;
    }

    public T credential() {
      return credential;
    }

    public boolean readyToRenew() {
      return staleness.readyToRenew(credential, clock, renewalLeadTimeMillis);
    }
  }
}
