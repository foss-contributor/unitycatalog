package io.unitycatalog.hadoop.internal.auth;

import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.internal.Preconditions;
import java.util.Objects;

/**
 * A {@link GenericCredential} together with the storage-path {@code prefix} it is scoped to.
 *
 * <p>I'm including this for now because eventually I will refactor GenericCredential to no longer
 * hold a temporary credential. This is just here for now to minimize the code that needs to be
 * reviewed.
 */
public final class GenericStorageCredential {
  private final GenericCredential credential;
  private final String prefix;

  public GenericStorageCredential(GenericCredential credential, String prefix) {
    Preconditions.checkNotNull(credential, "credential is required");
    this.credential = credential;
    this.prefix = prefix;
  }

  public GenericCredential credential() {
    return credential;
  }

  /** The storage path prefix this credential is scoped to, or {@code null} if not scoped. */
  public String prefix() {
    return prefix;
  }

  public boolean readyToRenew(Clock clock, long renewalLeadTimeMillis) {
    return credential.readyToRenew(clock, renewalLeadTimeMillis);
  }

  @Override
  public int hashCode() {
    return Objects.hash(credential, prefix);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GenericStorageCredential that = (GenericStorageCredential) o;
    return Objects.equals(credential, that.credential) && Objects.equals(prefix, that.prefix);
  }

  @Override
  public String toString() {
    return "GenericStorageCredential{prefix=" + prefix + "}";
  }
}
