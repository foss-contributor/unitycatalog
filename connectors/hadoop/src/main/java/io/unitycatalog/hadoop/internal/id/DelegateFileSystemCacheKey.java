package io.unitycatalog.hadoop.internal.id;

import static io.unitycatalog.hadoop.internal.UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY;

import io.unitycatalog.client.internal.Preconditions;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.hadoop.conf.Configuration;

/**
 * Cache key that pairs a {@link CredId} with the storage {@code location} a request targets. I will
 * refactor the CredId with a better name ie CredScope. Because now the CredId represents HOW to get
 * the LIST of credentials associated with a "table".
 */
public final class DelegateFileSystemCacheKey {
  private final CredId credId; // we have this here essentially as "how" to fetch creds
  private final String location; // this is how we select the right cred for this delegate FS

  public DelegateFileSystemCacheKey(CredId credId, String location) {
    Preconditions.checkNotNull(credId, "credId is required");
    this.credId = credId;
    this.location = location; // nullable, only because the all.yaml change PR pending.
  }

  /**
   * Builds a key from Hadoop configuration: the {@link CredId} via {@link CredId#create} and the
   * location from {@code UC_CREDENTIAL_LOCATION_KEY} (absent for single-credential scopes).
   */
  public static DelegateFileSystemCacheKey create(Configuration conf) {
    return new DelegateFileSystemCacheKey(
        CredId.create(conf), conf.get(UC_CREDENTIAL_LOCATION_KEY));
  }

  /** Builds a key from configuration, using {@code defaultCredId} when no UC scope is present. */
  public static DelegateFileSystemCacheKey create(
      Configuration conf, Supplier<CredId> defaultCredId) {
    return new DelegateFileSystemCacheKey(
        CredId.create(conf, defaultCredId), conf.get(UC_CREDENTIAL_LOCATION_KEY));
  }

  public CredId credId() {
    return credId;
  }

  public String location() {
    return location;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DelegateFileSystemCacheKey)) return false;
    DelegateFileSystemCacheKey that = (DelegateFileSystemCacheKey) o;
    return Objects.equals(credId, that.credId) && Objects.equals(location, that.location);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credId, location);
  }

  @Override
  public String toString() {
    return "DelegateFileSystemCacheKey{credId=" + credId + ", location=" + location + "}";
  }
}
