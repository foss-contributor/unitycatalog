package io.unitycatalog.hadoop.internal;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.auth.TokenProvider;
import io.unitycatalog.client.internal.ApiClientUtils;
import io.unitycatalog.client.internal.Clock;
import io.unitycatalog.client.internal.Preconditions;
import io.unitycatalog.client.model.AwsCredentials;
import io.unitycatalog.client.model.AzureUserDelegationSAS;
import io.unitycatalog.client.model.GcpOauthToken;
import io.unitycatalog.client.model.TemporaryCredentials;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs;
import io.unitycatalog.hadoop.internal.auth.CredentialCache;
import io.unitycatalog.hadoop.internal.auth.CredentialCache.RenewableCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.auth.GenericStorageCredential;
import io.unitycatalog.hadoop.internal.id.CredId;
import io.unitycatalog.hadoop.internal.id.DeltaStagingTableCredId;
import io.unitycatalog.hadoop.internal.id.DeltaTableCredId;
import io.unitycatalog.hadoop.internal.id.PathCredId;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import io.unitycatalog.hadoop.internal.util.ClockUtil;
import io.unitycatalog.hadoop.internal.util.MapIdGenerator;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.conf.Configuration;

/**
 * Internal utility that builds cloud-provider specific Hadoop configuration properties for Unity
 * Catalog vended credentials.
 *
 * <p><b>This is an internal class and is not part of the public API.</b> Use {@link
 * UCCredentialHadoopConfs} instead.
 */
public class CredPropsUtil {
  private CredPropsUtil() {}

  /**
   * Factory seam for {@link GenericCredentialFetcher#create(ApiClient, CredId)}, swappable from
   * tests so the fetch methods can be exercised without a real UC server. Test-only; production
   * code must not depend on the swap behavior.
   */
  @FunctionalInterface
  public interface GenericCredentialFetcherFactory {
    GenericCredentialFetcher create(ApiClient apiClient, CredId credId);
  }

  public static volatile GenericCredentialFetcherFactory genericCredFetcherFactory =
      GenericCredentialFetcher::create;

  static final CredentialCache<CredId, List<GenericStorageCredential>> initialCredCache =
      CredentialCache.createInitialCredentialCache();

  private static final String CRED_SCOPED_FS_CLASS =
      "io.unitycatalog.hadoop.internal.fs.CredScopedFileSystem";
  private static final String CRED_SCOPED_AFS_CLASS =
      "io.unitycatalog.hadoop.internal.fs.CredScopedFs";
  private static final String AWS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.AwsVendedTokenProvider";
  private static final String GCS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.GcsVendedTokenProvider";
  private static final String ABFS_VENDED_TOKEN_PROVIDER_CLASS =
      "io.unitycatalog.hadoop.internal.auth.AbfsVendedTokenProvider";
  private static final String GCS_ACCESS_TOKEN_KEY = "fs.gs.auth.access.token.credential";
  private static final String GCS_ACCESS_TOKEN_EXPIRATION_KEY =
      "fs.gs.auth.access.token.expiration";
  private static final String GCS_CONFLICT_CHECK_KEY = "fs.gs.create.items.conflict.check.enable";
  private static final String ABFS_FIXED_SAS_TOKEN_KEY = "fs.azure.sas.fixed.token";

  // Keys used to build the credential-context id (see #credContextId).
  private static final String CRED_CONTEXT_CATALOG_URI_KEY = "catalogUri";
  private static final String CRED_CONTEXT_SCHEME_KEY = "scheme";

  private abstract static class PropsBuilder<T extends PropsBuilder<T>> {
    private final HashMap<String, String> builder = new HashMap<>();

    public T set(String key, String value) {
      builder.put(key, value);
      return self();
    }

    public T uri(String uri) {
      builder.put(UCHadoopConfConstants.UC_URI_KEY, uri);
      return self();
    }

    public T tokenProvider(TokenProvider tokenProvider) {
      // As we can only propagate the properties with prefix 'fs.*' to the FileSystem
      // implementation. So let's add the prefix here.
      tokenProvider
          .configs()
          .forEach((key, value) -> builder.put(UCHadoopConfConstants.UC_AUTH_PREFIX + key, value));
      return self();
    }

    /** Applies the credential-scope identity properties carried by {@code credId}. */
    public T credId(CredId credId) {
      credId.props().forEach(this::set);
      return self();
    }

    public T appVersions(Map<String, String> appVersions) {
      appVersions.forEach(
          (k, v) -> builder.put(UCHadoopConfConstants.UC_ENGINE_VERSION_PREFIX + k, v));
      return self();
    }

    /**
     * Saves the current value of {@code key} from {@code hadoopProps} (falling back to {@code
     * defaultOriginal}) under {@code key + ".original"}, then overrides {@code key} with {@code
     * newValue}. This lets CredScopedFileSystem#newFileSystem restore the real delegate
     * implementation after the wrapper has been installed.
     */
    public T saveAndOverride(
        Configuration hadoopConf, String key, String defaultOriginal, String newValue) {
      builder.put(key + ".original", hadoopConf.get(key, defaultOriginal));
      builder.put(key, newValue);
      return self();
    }

    /**
     * When {@code credScopedFsEnabled}, overrides {@code fs.<scheme>.impl} (and the abstract-fs
     * variants) with {@code CredScopedFileSystem}, saving the real implementation via {@link
     * #saveAndOverride} so the executor can restore it. A no-op otherwise. Terminal step: applied
     * uniformly regardless of how the credential is serialized.
     */
    public T setOverrides(boolean credScopedFsEnabled, Configuration hadoopConf) {
      if (credScopedFsEnabled) {
        applyImplOverrides(hadoopConf);
      }
      return self();
    }

    /** Installs this scheme's {@code fs.<scheme>.impl} overrides. */
    protected abstract void applyImplOverrides(Configuration hadoopConf);

    protected abstract T self();

    public Map<String, String> build() {
      return Collections.unmodifiableMap(new HashMap<>(builder));
    }
  }

  static class S3PropsBuilder extends PropsBuilder<S3PropsBuilder> {

    S3PropsBuilder() {
      // Common properties for S3.
      set("fs.s3a.path.style.access", "true");
      set("fs.s3.impl.disable.cache", "true");
      set("fs.s3a.impl.disable.cache", "true");
    }

    @Override
    protected void applyImplOverrides(Configuration hadoopConf) {
      // i want to further clean this up to move all of the builder out of this file
      saveAndOverride(
          hadoopConf, "fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem", CRED_SCOPED_FS_CLASS);
      saveAndOverride(
          hadoopConf,
          "fs.s3a.impl",
          "org.apache.hadoop.fs.s3a.S3AFileSystem",
          CRED_SCOPED_FS_CLASS);
      saveAndOverride(
          hadoopConf,
          "fs.AbstractFileSystem.s3.impl",
          "org.apache.hadoop.fs.s3a.S3A",
          CRED_SCOPED_AFS_CLASS);
      saveAndOverride(
          hadoopConf,
          "fs.AbstractFileSystem.s3a.impl",
          "org.apache.hadoop.fs.s3a.S3A",
          CRED_SCOPED_AFS_CLASS);
    }

    @Override
    protected S3PropsBuilder self() {
      return this;
    }
  }

  private static class GcsPropsBuilder extends PropsBuilder<GcsPropsBuilder> {

    GcsPropsBuilder(Configuration hadoopConf) {
      // The upstream GCS connector defaults this to true which causes the connector to
      // stat every ancestor directory on file creation. With UC-vended downscoped tokens
      // (scoped to a table's path prefix) these ancestor stats return 403. Default to
      // false; users with broader credentials can opt back in via Hadoop/Spark config.
      set(GCS_CONFLICT_CHECK_KEY, hadoopConf.get(GCS_CONFLICT_CHECK_KEY, "false"));
      set("fs.gs.impl.disable.cache", "true");
    }

    @Override
    protected void applyImplOverrides(Configuration hadoopConf) {
      saveAndOverride(
          hadoopConf,
          "fs.gs.impl",
          "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
          CRED_SCOPED_FS_CLASS);
      saveAndOverride(
          hadoopConf,
          "fs.AbstractFileSystem.gs.impl",
          "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
          CRED_SCOPED_AFS_CLASS);
    }

    @Override
    protected GcsPropsBuilder self() {
      return this;
    }
  }

  private static class AbfsPropsBuilder extends PropsBuilder<AbfsPropsBuilder> {

    AbfsPropsBuilder() {
      set(UCHadoopConfConstants.FS_AZURE_ACCOUNT_AUTH_TYPE_PROPERTY_NAME, "SAS");
      set(UCHadoopConfConstants.FS_AZURE_ACCOUNT_IS_HNS_ENABLED, "true");
      set("fs.abfs.impl.disable.cache", "true");
      set("fs.abfss.impl.disable.cache", "true");
    }

    @Override
    protected void applyImplOverrides(Configuration hadoopConf) {
      saveAndOverride(
          hadoopConf,
          "fs.abfs.impl",
          "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
          CRED_SCOPED_FS_CLASS);
      saveAndOverride(
          hadoopConf,
          "fs.abfss.impl",
          "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
          CRED_SCOPED_FS_CLASS);
      saveAndOverride(
          hadoopConf,
          "fs.AbstractFileSystem.abfs.impl",
          "org.apache.hadoop.fs.azurebfs.Abfs",
          CRED_SCOPED_AFS_CLASS);
      saveAndOverride(
          hadoopConf,
          "fs.AbstractFileSystem.abfss.impl",
          "org.apache.hadoop.fs.azurebfs.Abfss",
          CRED_SCOPED_AFS_CLASS);
    }

    @Override
    protected AbfsPropsBuilder self() {
      return this;
    }
  }

  /** Fetches table credentials from the UC REST API and builds Hadoop configuration properties. */
  public static Map<String, String> createTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      String tableId,
      UCCredentialHadoopConfs.TableOperation tableOp,
      Map<String, String> appVersions)
      throws ApiException {
    return createCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        hadoopConf,
        scheme,
        apiClient,
        catalogUri,
        tokenProvider,
        appVersions,
        new TableCredId(
            credContextId(catalogUri, scheme, tokenProvider), tableId, tableOp.value()));
  }

  /**
   * Fetches Delta table credentials from the UC Delta API and builds Hadoop configuration
   * properties.
   */
  public static Map<String, String> createDeltaTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      UCDeltaTableIdentifier identifier,
      String location,
      UCCredentialHadoopConfs.TableOperation tableOp,
      Map<String, String> appVersions)
      throws ApiException {
    return createCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        hadoopConf,
        scheme,
        apiClient,
        catalogUri,
        tokenProvider,
        appVersions,
        new DeltaTableCredId(
            credContextId(catalogUri, scheme, tokenProvider),
            identifier,
            tableOp.value(),
            location));
  }

  /**
   * Fetches Delta staging table credentials from the UC Delta API and builds Hadoop configuration
   * properties.
   */
  public static Map<String, String> createDeltaStagingTableCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      String stagingTableId,
      String location,
      Map<String, String> appVersions)
      throws ApiException {
    return createCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        hadoopConf,
        scheme,
        apiClient,
        catalogUri,
        tokenProvider,
        appVersions,
        new DeltaStagingTableCredId(
            credContextId(catalogUri, scheme, tokenProvider), stagingTableId, location));
  }

  /** Fetches path credentials from the UC REST API and builds Hadoop configuration properties. */
  public static Map<String, String> createPathCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      String path,
      UCCredentialHadoopConfs.PathOperation pathOp,
      Map<String, String> appVersions)
      throws ApiException {
    return createCredProps(
        renewCredEnabled,
        credScopedFsEnabled,
        hadoopConf,
        scheme,
        apiClient,
        catalogUri,
        tokenProvider,
        appVersions,
        new PathCredId(credContextId(catalogUri, scheme, tokenProvider), path, pathOp.value()));
  }

  /**
   * Fetches temporary credentials for {@code credId} and builds the cloud-provider specific Hadoop
   * configuration properties for {@code scheme}. Shared by all {@code create*CredProps} entry
   * points, which differ only in how they construct the {@link CredId}.
   */
  private static Map<String, String> createCredProps(
      boolean renewCredEnabled,
      boolean credScopedFsEnabled,
      Configuration hadoopConf,
      String scheme,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId)
      throws ApiException {
    List<GenericStorageCredential> creds =
        fetchGenericCredentials(
            hadoopConf, apiClient, catalogUri, tokenProvider, appVersions, credId);

    // The driver must vend at least one credential for the executor to serialize; zero would leave
    // the executor with no credential to select and is always a bug upstream.
    Preconditions.checkArgument(!creds.isEmpty(), "UC vended no credentials for '%s'.", credId);

    if (!credScopedFsEnabled) {
      // Without the credential-scoped filesystem there is no keyspace to isolate credentials, so
      // exactly one credential must be vended and it is written at the top level.
      Preconditions.checkArgument(
          creds.size() == 1,
          "Received %s credentials for '%s' but the credential-scoped filesystem is disabled; "
              + "cannot isolate multiple credentials.",
          creds.size(),
          credId);
      return topLevelCredProps(
          renewCredEnabled,
          hadoopConf,
          scheme,
          catalogUri,
          tokenProvider,
          appVersions,
          credId,
          creds.get(0));
    }

    return keyspaceCredProps(
        renewCredEnabled,
        hadoopConf,
        scheme,
        catalogUri,
        tokenProvider,
        appVersions,
        credId,
        creds);
  }

  /**
   * Builds the top-level Hadoop properties for a single vended credential when the
   * credential-scoped filesystem is disabled: the credential's value keys plus (for renewable
   * credentials) the vended token provider wiring, request scope, and engine versions — all written
   * directly at the top level (prefix {@code ""}).
   */
  private static Map<String, String> topLevelCredProps(
      boolean renewCredEnabled,
      Configuration hadoopConf,
      String scheme,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId,
      GenericStorageCredential scopedCred) {
    PropsBuilder<?> builder = baseBuilder(scheme, hadoopConf);
    if (builder == null) {
      return Collections.emptyMap();
    }
    if (renewCredEnabled) {
      applySharedContext(builder, scheme, catalogUri, tokenProvider, appVersions, credId);
    }

    Map<String, String> props = new HashMap<>(builder.build());
    writeValueKeys(props, "", renewCredEnabled, scheme, scopedCred.credential());

    // Record the credential's location (when scoped) so the executor can confirm it covers the
    // request. A single-credential response may still carry a prefix (a Delta table with one
    // covering credential); UC table/path credentials have no prefix and omit the key.
    if (scopedCred.prefix() != null) {
      props.put(UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, scopedCred.prefix());
    }
    return Collections.unmodifiableMap(props);
  }

  /**
   * Serializes one or more vended credentials into the credential keyspace consumed by {@code
   * CredScopedFileSystem}. The shared, credential-value-free context (filesystem impl overrides,
   * and for renewable credentials the vended token provider wiring, request URI/scope, and engine
   * versions) is written once at the top level; each credential's location and value keys go under
   * its own {@code fs.unitycatalog.scoped.cred.<i>.} namespace, so the executor overlays one
   * namespace to reconstruct a single-credential conf.
   *
   * <p>The request's {@link CredId} scope is top-level identity used to key caches, not credential
   * material. Multiple credentials are only produced by prefix-scoped UC Delta table responses,
   * which are always renewable.
   */
  private static Map<String, String> keyspaceCredProps(
      boolean renewCredEnabled,
      Configuration hadoopConf,
      String scheme,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId,
      List<GenericStorageCredential> creds) {
    if (creds.size() > 1) { // this might not be necessary; cleanup later
      Preconditions.checkArgument(
          credId instanceof DeltaTableCredId,
          "Multi-credential responses are only supported for Delta table scopes, got: %s",
          credId);
    }

    PropsBuilder<?> builder = baseBuilder(scheme, hadoopConf);
    if (builder == null) {
      // Unknown scheme; mirror the top-level path's default of no properties.
      return Collections.emptyMap();
    }
    builder.setOverrides(true, hadoopConf);
    if (renewCredEnabled) {
      applySharedContext(builder, scheme, catalogUri, tokenProvider, appVersions, credId);
    }

    Map<String, String> props = new HashMap<>(builder.build());
    props.put(UCHadoopConfConstants.UC_SCOPED_CRED_COUNT_KEY, String.valueOf(creds.size()));

    for (int i = 0; i < creds.size(); i++) {
      GenericStorageCredential scopedCred = creds.get(i);
      String namespace = UCHadoopConfConstants.UC_SCOPED_CRED_PREFIX + i + ".";
      if (scopedCred.prefix() != null) {
        props.put(
            namespace + UCHadoopConfConstants.UC_CREDENTIAL_LOCATION_KEY, scopedCred.prefix());
      }
      writeValueKeys(props, namespace, renewCredEnabled, scheme, scopedCred.credential());
    }
    return Collections.unmodifiableMap(props);
  }

  /** Returns the minimal base builder for {@code scheme}, or {@code null} if unrecognized. */
  private static PropsBuilder<?> baseBuilder(String scheme, Configuration hadoopConf) {
    switch (scheme) {
      case "s3":
        return new S3PropsBuilder();
      case "gs":
        return new GcsPropsBuilder(hadoopConf);
      case "abfss":
      case "abfs":
        return new AbfsPropsBuilder();
      default:
        return null;
    }
  }

  /**
   * Applies the shared, credential-value-free context for a renewable credential to {@code
   * builder}: the vended token provider class wiring, request URI, auth configs, request {@link
   * CredId} scope, and engine versions. Written once (at the top level) regardless of how many
   * credentials the response carries; the per-credential value keys are written separately by
   * {@link #writeValueKeys}.
   */
  private static void applySharedContext(
      PropsBuilder<?> builder,
      String scheme,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId) {
    switch (scheme) {
      case "s3":
        builder.set(
            UCHadoopConfConstants.S3A_CREDENTIALS_PROVIDER, AWS_VENDED_TOKEN_PROVIDER_CLASS);
        break;
      case "gs":
        builder
            .set("fs.gs.auth.type", "ACCESS_TOKEN_PROVIDER")
            .set("fs.gs.auth.access.token.provider", GCS_VENDED_TOKEN_PROVIDER_CLASS);
        break;
      case "abfss":
      case "abfs":
        builder.set(
            UCHadoopConfConstants.FS_AZURE_SAS_TOKEN_PROVIDER_TYPE,
            ABFS_VENDED_TOKEN_PROVIDER_CLASS);
        break;
      default:
        break;
    }
    builder.uri(catalogUri).tokenProvider(tokenProvider).credId(credId).appVersions(appVersions);
  }

  /**
   * Writes one credential's value keys under {@code prefix} ({@code ""} for the top level, or a
   * {@code fs.unitycatalog.scoped.cred.<i>.} namespace). Renewable credentials get the {@code
   * fs.<scheme>.init.*} keys the vended token provider reads on refresh; fixed credentials get the
   * raw {@code fs.<scheme>} auth keys. A single writer serves both the top-level and keyspace paths
   * so their layouts cannot drift.
   */
  private static void writeValueKeys(
      Map<String, String> props,
      String prefix,
      boolean renewCredEnabled,
      String scheme,
      GenericCredential cred) {
    TemporaryCredentials tempCreds = cred.temporaryCredentials();
    Long expiration = tempCreds.getExpirationTime();
    switch (scheme) {
      case "s3":
        AwsCredentials awsCred = tempCreds.getAwsTempCredentials();
        if (renewCredEnabled) {
          props.put(prefix + UCHadoopConfConstants.S3A_INIT_ACCESS_KEY, awsCred.getAccessKeyId());
          props.put(
              prefix + UCHadoopConfConstants.S3A_INIT_SECRET_KEY, awsCred.getSecretAccessKey());
          props.put(
              prefix + UCHadoopConfConstants.S3A_INIT_SESSION_TOKEN, awsCred.getSessionToken());
          if (expiration != null) {
            props.put(
                prefix + UCHadoopConfConstants.S3A_INIT_CRED_EXPIRED_TIME,
                String.valueOf(expiration));
          }
        } else {
          props.put(prefix + "fs.s3a.access.key", awsCred.getAccessKeyId());
          props.put(prefix + "fs.s3a.secret.key", awsCred.getSecretAccessKey());
          props.put(prefix + "fs.s3a.session.token", awsCred.getSessionToken());
        }
        break;
      case "gs":
        GcpOauthToken gcpToken = tempCreds.getGcpOauthToken();
        if (renewCredEnabled) {
          props.put(prefix + UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN, gcpToken.getOauthToken());
          if (expiration != null) {
            props.put(
                prefix + UCHadoopConfConstants.GCS_INIT_OAUTH_TOKEN_EXPIRATION_TIME,
                String.valueOf(expiration));
          }
        } else {
          Long gsExpiration = expiration == null ? Long.MAX_VALUE : expiration;
          props.put(prefix + GCS_ACCESS_TOKEN_KEY, gcpToken.getOauthToken());
          props.put(prefix + GCS_ACCESS_TOKEN_EXPIRATION_KEY, String.valueOf(gsExpiration));
        }
        break;
      case "abfss":
      case "abfs":
        AzureUserDelegationSAS azureSas = tempCreds.getAzureUserDelegationSas();
        if (renewCredEnabled) {
          props.put(prefix + UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN, azureSas.getSasToken());
          if (expiration != null) {
            props.put(
                prefix + UCHadoopConfConstants.AZURE_INIT_SAS_TOKEN_EXPIRED_TIME,
                String.valueOf(expiration));
          }
        } else {
          props.put(prefix + ABFS_FIXED_SAS_TOKEN_KEY, azureSas.getSasToken());
        }
        break;
      default:
        // Unrecognized scheme: no value keys (base props are already empty for this case).
        break;
    }
  }

  /**
   * Derives a stable id for the credential context so caches only reuse a vended credential across
   * requests that would receive the same one. A vended credential is a function of the catalog
   * endpoint, storage scheme, and auth identity, so all three are folded into the hash alongside
   * the auth configs from {@code tokenProvider}.
   */
  static String credContextId(String catalogUri, String scheme, TokenProvider tokenProvider) {
    Objects.requireNonNull(catalogUri, "catalogUri is required");
    Objects.requireNonNull(scheme, "scheme is required");
    Objects.requireNonNull(tokenProvider, "tokenProvider is required");

    Map<String, String> context = new HashMap<>(tokenProvider.configs());
    context.put(CRED_CONTEXT_CATALOG_URI_KEY, catalogUri);
    context.put(CRED_CONTEXT_SCHEME_KEY, scheme);
    return MapIdGenerator.generateId(context);
  }

  private static List<GenericStorageCredential> fetchGenericCredentials(
      Configuration hadoopConf,
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId)
      throws ApiException {
    boolean credCacheEnabled =
        hadoopConf.getBoolean(
            UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_KEY,
            UCHadoopConfConstants.UC_CREDENTIAL_CACHE_ENABLED_DEFAULT_VALUE);
    if (!credCacheEnabled) {
      return createCredentials(apiClient, catalogUri, tokenProvider, appVersions, credId);
    }

    long renewalLeadTimeMillis =
        hadoopConf.getLong(
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_KEY,
            UCHadoopConfConstants.UC_RENEWAL_LEAD_TIME_DEFAULT_VALUE);
    // i have a concern where we could have false cache hits:
    // if a table's location and identifier don't change, but the list of credentials it vends
    // changes, ie somehow the location changes without changing the table's stored location,
    // we will keep using the same outdated credentials. this shouldn't be a real conern though.
    return initialCredCache.access(
        credId,
        () ->
            new RenewableCredential<>(
                renewalLeadTimeMillis,
                ClockUtil.resolveClock(hadoopConf),
                createCredentials(apiClient, catalogUri, tokenProvider, appVersions, credId),
                CredPropsUtil::credentialsReadyToRenew));
  }

  /** A batch is ready to renew as soon as any credential in it is ready to renew. */
  private static boolean credentialsReadyToRenew(
      List<GenericStorageCredential> creds, Clock clock, long renewalLeadTimeMillis) {
    return creds.stream().anyMatch(c -> c.readyToRenew(clock, renewalLeadTimeMillis));
  }

  private static List<GenericStorageCredential> createCredentials(
      ApiClient apiClient,
      String catalogUri,
      TokenProvider tokenProvider,
      Map<String, String> appVersions,
      CredId credId)
      throws ApiException {
    ApiClient client =
        apiClient != null ? apiClient : createApiClient(catalogUri, tokenProvider, appVersions);
    return genericCredFetcherFactory.create(client, credId).createCredentials();
  }

  private static ApiClient createApiClient(
      String catalogUri, TokenProvider tokenProvider, Map<String, String> appVersions) {
    return ApiClientUtils.create(
        URI.create(catalogUri),
        tokenProvider,
        UCHadoopConfConstants.createRequestRetryPolicy(null),
        appVersions);
  }
}
