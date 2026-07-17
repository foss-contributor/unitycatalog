package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.hadoop.internal.StorageLocationUtil;
import io.unitycatalog.hadoop.internal.UCHadoopConfConstants;
import io.unitycatalog.hadoop.internal.auth.CredentialCache.RenewableCredential;
import io.unitycatalog.hadoop.internal.id.DelegateFileSystemCacheKey;
import io.unitycatalog.hadoop.internal.util.ClockUtil;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;

/**
 * Base class for Hadoop credential providers backed by Unity Catalog temporary credentials.
 *
 * <p>Subclasses expose cloud-specific provider interfaces while this class handles renewal and
 * cache lookup.
 */
public abstract class GenericCredentialProvider {
  static final CredentialCache<DelegateFileSystemCacheKey, GenericCredential> globalCache =
      CredentialCache.createGlobalCache();

  private Configuration conf;
  private Clock clock;
  private long renewalLeadTimeMillis;
  private DelegateFileSystemCacheKey cacheKey;
  private boolean credCacheEnabled;

  private volatile GenericCredential credential;
  private volatile GenericCredentialFetcher credentialFetcher;

  protected void initialize(Configuration conf) {
    this.conf = conf;
    this.clock = ClockUtil.resolveClock(conf);

    this.renewalLeadTimeMillis =
        conf.getLong(
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY,
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);

    // This cache key is used to identify the delegate (real) file system.
    // each file system only has one associated credential so it carries the
    // file system location (URI) for the cases where the fetcher returns multiple credentials
    // so it's able to select the correct credential. Note that this location could be null
    // in the cases where the fetcher returns a single credential as no selection is required.
    // If the fetcher returns multiple credentials AND the location is null, throw an error
    // as we don't know which credential to select.
    this.cacheKey = DelegateFileSystemCacheKey.create(conf);

    this.credCacheEnabled =
        conf.getBoolean(
            UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY,
            UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE);

    // The initialized credentials passing-through the hadoop configuration.
    this.credential = initGenericCredential(conf);
  }

  public abstract GenericCredential initGenericCredential(Configuration conf);

  public GenericCredential accessCredentials() {
    if (credential == null || credential.readyToRenew(clock, renewalLeadTimeMillis)) {
      synchronized (this) {
        if (credential == null || credential.readyToRenew(clock, renewalLeadTimeMillis)) {
          try {
            credential = renewCredential();
          } catch (ApiException e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    return credential;
  }

  GenericCredentialFetcher genericCredentialFetcher() {
    if (credentialFetcher == null) {
      synchronized (this) {
        if (credentialFetcher == null) {
          credentialFetcher = GenericCredentialFetcher.create(conf);
        }
      }
    }
    return credentialFetcher;
  }

  private GenericCredential renewCredential() throws ApiException {
    if (credCacheEnabled) {
      return globalCache.access(
          cacheKey,
          () ->
              new RenewableCredential<>(
                  renewalLeadTimeMillis, clock, fetchAndSelect(), GenericCredential::readyToRenew));
    } else {
      return fetchAndSelect();
    }
  }

  /**
   * Fetches the vended credentials for this scope and selects the one that applies. A response may
   * carry several prefix-scoped credentials (UC Delta); the one whose prefix covers the request
   * location (from {@link DelegateFileSystemCacheKey#location()}) is chosen.
   */
  private GenericCredential fetchAndSelect() throws ApiException {
    List<GenericStorageCredential> creds = genericCredentialFetcher().createCredentials();
    String location = cacheKey.location();
    if (location == null) {
      Preconditions.checkArgument(
          creds.size() == 1,
          "Expected exactly one credential for scope %s but got %s.",
          cacheKey,
          creds.size());
      return creds.get(0).credential();
    }

    List<String> prefixes = new ArrayList<>(creds.size());
    for (GenericStorageCredential cred : creds) {
      prefixes.add(cred.prefix());
    }
    int match = StorageLocationUtil.longestCoveringIndex(location, prefixes);
    Preconditions.checkArgument(
        match >= 0, "No vended credential prefix covers location '%s'.", location);
    return creds.get(match).credential();
  }
}
