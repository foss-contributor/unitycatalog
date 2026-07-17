package io.unitycatalog.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.spark.UCSingleCatalog;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.List;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * End-to-end read test against a remote Unity Catalog server for a table whose metadata and data
 * files are stored under multiple location prefixes.
 *
 * <p>Such a table accesses its Delta log under one storage prefix and data files under another.
 * Reading it therefore requires two prefix-scoped credentials, which the UC Delta credentials API
 * vends as a multi-element {@code storage-credentials} list. This test exercises the full
 * connector path end to end: {@link UCSingleCatalog} fetches the credentials, {@code
 * CredPropsUtil} serializes all of them into the multi-credential keyspace, and {@code
 * CredScopedFileSystem} selects the credential whose prefix covers each accessed URI. A successful
 * read proves both prefixes were served their own scoped credential.
 *
 * <p>This test talks to a real remote UC and real cloud storage, so it is disabled unless {@code
 * CATALOG_URI} is set. It never runs in CI.
 *
 * <p><b>To run:</b> {@code CATALOG_URI} is the bare catalog server host (the connector's API
 * client appends {@code /api/2.1/unity-catalog} itself); {@code CATALOG_AUTH_TOKEN} is an
 * authentication token.
 *
 * <pre>
 * export CATALOG_URI="https://&lt;catalog-server-host&gt;"
 * export CATALOG_AUTH_TOKEN="&lt;catalog-token&gt;"
 * export CATALOG_NAME=&lt;catalog-name&gt;
 * export TABLE_NAME=&lt;catalog.schema.table&gt;
 *
 * # NOTE: the connector only calls the multi-credential UC Delta REST API when the Delta library
 * # on the classpath is &gt;= 4.3.0 (see DeltaVersionUtils.MIN_DELTA_VERSION_FOR_UC_DELTA_API). The
 * # default build uses Delta 4.2.0, which falls back to the single-credential path. Run with a
 * # Delta &gt;= 4.3.0 build to exercise the multiple-location table path:
 * SBT_OPTS="-Xmx8G -XX:+UseG1GC" ./build/sbt -DdeltaVersion=4.3.0 \
 *   "integrationTests/testOnly io.unitycatalog.integrationtests.MultipleLocationTableReadE2ETest"
 * </pre>
 */
@EnabledIfEnvironmentVariable(named = "CATALOG_URI", matches = ".+")
public class MultipleLocationTableReadE2ETest {

  private static final String CATALOG_NAME = TestUtils.envAsString("CATALOG_NAME", "test_catalog");
  private static final String CATALOG_URI = TestUtils.envAsString("CATALOG_URI", null);
  private static final String AUTH_TOKEN = TestUtils.envAsString("CATALOG_AUTH_TOKEN", "");
  // Fully-qualified name of the multiple-location table to read.
  private static final String TABLE_NAME =
      TestUtils.envAsString(
          "TABLE_NAME", "test_catalog.test_schema.multiple_location_table");

  private SparkSession spark;

  private SparkSession createSession() {
    String catalogKey = "spark.sql.catalog." + CATALOG_NAME;
    return SparkSession.builder()
        .appName("multiple-location-table-read-e2e")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        // Route unqualified (schema.table) references to the UC catalog by default.
        .config("spark.sql.defaultCatalog", CATALOG_NAME)
        // Real S3 access uses the S3A filesystem; the UC-vended session credentials are wired into
        // it by the connector's AwsVendedTokenProvider.
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(catalogKey, UCSingleCatalog.class.getName())
        .config(catalogKey + "." + OptionsUtil.URI, CATALOG_URI)
        .config(catalogKey + "." + OptionsUtil.TOKEN, AUTH_TOKEN)
        .config(catalogKey + "." + OptionsUtil.WAREHOUSE, CATALOG_NAME)
        // Multi-credential isolation requires the credential-scoped filesystem.
        .config(catalogKey + "." + OptionsUtil.CRED_SCOPED_FS_ENABLED, "true")
        // Use the UC Delta REST credentials API (the multi-credential, list-valued endpoint).
        .config(catalogKey + "." + OptionsUtil.DELTA_API_ENABLED, "true")
        .getOrCreate();
  }

  @AfterEach
  public void afterEach() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
  }

  @Test
  public void readsTableAcrossMultipleLocations() {
    spark = createSession();

    // A full scan forces reads of the table's log prefix and data-file prefix, so both vended
    // credentials must resolve for this to succeed.
    List<Row> countRows = spark.sql("SELECT COUNT(*) FROM " + TABLE_NAME).collectAsList();
    assertThat(countRows).hasSize(1);
    long rowCount = countRows.get(0).getLong(0);
    assertThat(rowCount).isGreaterThanOrEqualTo(0L);

    // Materialize actual rows to force reading the underlying data files (not just metadata).
    List<Row> rows = spark.sql("SELECT * FROM " + TABLE_NAME + " LIMIT 10").collectAsList();
    assertThat(rows.size()).isEqualTo(Math.min(10L, rowCount));
  }
}
