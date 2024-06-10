/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.SkipDirectRunnerTest;
import com.google.cloud.teleport.metadata.TemplateIntegrationTest;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.function.Function;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.utils.PipelineUtils;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.TemplateTestBase;
import org.apache.beam.it.gcp.bigquery.BigQueryResourceManager;
import org.apache.beam.it.gcp.bigquery.conditions.BigQueryRowsCheck;
import org.apache.beam.it.gcp.bigquery.matchers.BigQueryAsserts;
import org.apache.beam.it.gcp.cloudsql.CloudMySQLResourceManager;
import org.apache.beam.it.gcp.cloudsql.CloudSqlResourceManager;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.jdbc.JDBCResourceManager;
import org.apache.commons.lang3.RandomStringUtils;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@Category({TemplateIntegrationTest.class, SkipDirectRunnerTest.class})
@TemplateIntegrationTest(DataStreamToBigQuery.class)
@RunWith(JUnit4.class)
public class DataStreamToBigQueryIT extends TemplateTestBase {

  private static final int NUM_EVENTS = 10;

  private static final String ROW_ID = "row_id";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String MEMBER = "member";
  private static final String ENTRY_ADDED = "entry_added";
  private static final List<String> COLUMNS = List.of(ROW_ID, NAME, AGE, MEMBER, ENTRY_ADDED);

  private String gcsPrefix;
  private String dlqGcsPrefix;

  private CloudSqlResourceManager cloudSqlResourceManager;
  private PubsubResourceManager pubsubResourceManager;
  private DatastreamResourceManager datastreamResourceManager;
  private BigQueryResourceManager bigQueryResourceManager;

  @Before
  public void setUp() throws IOException {
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, PROJECT, REGION)
            .setCredentialsProvider(credentialsProvider)
            .setPrivateConnectivity("datastream-private-connect-us-central1")
            .build();

    bigQueryResourceManager =
        BigQueryResourceManager.builder(testName, PROJECT, credentials).build();

    gcsClient.createArtifact(
        "udf.js",
        "function uppercaseName(value) {\n"
            + "  const data = JSON.parse(value);\n"
            + "  data.name = data.name.toUpperCase();\n"
            + "  return JSON.stringify(data);\n"
            + "}");

    gcsPrefix = getGcsPath(testName + "/cdc/").replace("gs://" + artifactBucketName, "");
    dlqGcsPrefix = getGcsPath(testName + "/dlq/").replace("gs://" + artifactBucketName, "");
  }

  @After
  public void cleanUp() {
    ResourceManagerUtils.cleanResources(
        cloudSqlResourceManager,
        pubsubResourceManager,
        datastreamResourceManager,
        bigQueryResourceManager);
  }

  @Test
  public void testDataStreamMySqlToBigQuery() throws IOException {
    // Run a simple IT
    simpleJdbcToBigQueryTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        config ->
            config
                .addParameter("inputFileFormat", "avro")
                .addParameter("gcsPubSubSubscription", ""));
  }

  @Test
  public void testDataStreamMySqlToBigQueryJson() throws IOException {
    // Run a simple IT
    simpleJdbcToBigQueryTest(
        DatastreamResourceManager.DestinationOutputFormat.JSON_FILE_FORMAT,
        config ->
            config
                .addParameter("inputFileFormat", "json")
                .addParameter("gcsPubSubSubscription", ""));
  }

  @Test
  public void testDataStreamMySqlToSpannerGCSNotifications() throws IOException {
    // Set up pubsub notifications
    SubscriptionName subscriptionName = createGcsNotifications();

    // Run a simple IT
    simpleJdbcToBigQueryTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        config ->
            config
                .addParameter("inputFileFormat", "avro")
                .addParameter("gcsPubSubSubscription", subscriptionName.toString()));
  }

  @Test
  public void testDataStreamToBigQueryUsingAtLeastOnceMode() throws IOException {
    // Set up pubsub notifications
    SubscriptionName subscriptionName = createGcsNotifications();

    ArrayList<String> experiments = new ArrayList<>();
    experiments.add("streaming_mode_at_least_once");
    simpleJdbcToBigQueryTest(
        DatastreamResourceManager.DestinationOutputFormat.AVRO_FILE_FORMAT,
        config ->
            config
                .addParameter("inputFileFormat", "avro")
                .addParameter("gcsPubSubSubscription", subscriptionName.toString())
                .addEnvironment("additionalExperiments", experiments)
                .addEnvironment("enableStreamingEngine", true));
  }

  private void simpleJdbcToBigQueryTest(
      DatastreamResourceManager.DestinationOutputFormat fileFormat,
      Function<LaunchConfig.Builder, LaunchConfig.Builder> paramsAdder)
      throws IOException {

    // Create MySQL Resource manager
    cloudSqlResourceManager = CloudMySQLResourceManager.builder(testName).build();

    // Create JDBC tables
    String tableName = "MySqlToBigQuery_" + RandomStringUtils.randomAlphanumeric(5);
    cloudSqlResourceManager.createTable(tableName, createMySqlSchema());

    MySQLSource jdbcSource =
        MySQLSource.builder(
                cloudSqlResourceManager.getHost(),
                cloudSqlResourceManager.getUsername(),
                cloudSqlResourceManager.getPassword(),
                cloudSqlResourceManager.getPort())
            .build();

    // Create a BigQuery table
    List<Field> bqSchemaFields =
        Arrays.asList(
            Field.of(ROW_ID, StandardSQLTypeName.INT64),
            Field.of(NAME, StandardSQLTypeName.STRING),
            Field.of(AGE, StandardSQLTypeName.INT64),
            Field.of(MEMBER, StandardSQLTypeName.STRING),
            Field.of(ENTRY_ADDED, StandardSQLTypeName.STRING));
    Schema bqSchema = Schema.of(bqSchemaFields);
    bigQueryResourceManager.createDataset(REGION);
    TableId bqTable = bigQueryResourceManager.createTable(tableName, bqSchema);

    // Create Datastream JDBC Source Connection profile and config
    SourceConfig sourceConfig =
        datastreamResourceManager.buildJDBCSourceConfig("mysql-profile", jdbcSource);

    // Create Datastream GCS Destination Connection profile and config
    DestinationConfig destinationConfig =
        datastreamResourceManager.buildGCSDestinationConfig(
            "gcs-profile", artifactBucketName, gcsPrefix, fileFormat);

    // Create and start Datastream stream
    Stream stream =
        datastreamResourceManager.createStream("stream1", sourceConfig, destinationConfig);
    datastreamResourceManager.startStream(stream);

    // Construct template
    String jobName = PipelineUtils.createJobName(testName);
    PipelineLauncher.LaunchConfig.Builder options =
        paramsAdder
            .apply(
                PipelineLauncher.LaunchConfig.builder(jobName, specPath)
                    .addParameter("inputFilePattern", getGcsPath(testName) + "/cdc/")
                    .addParameter(
                        "outputStagingDatasetTemplate", bigQueryResourceManager.getDatasetId()))
            .addParameter("outputStagingTableNameTemplate", "{_metadata_table}_staging")
            .addParameter("outputDatasetTemplate", bigQueryResourceManager.getDatasetId())
            .addParameter("outputTableNameTemplate", "{_metadata_table}")
            .addParameter("streamName", stream.getName())
            .addParameter("deadLetterQueueDirectory", getGcsPath(testName) + "/dlq/")
            .addParameter("mergeFrequencyMinutes", "2")
            .addParameter("dlqRetryMinutes", "1")
            .addParameter("javascriptTextTransformGcsPath", getGcsPath("udf.js"))
            .addParameter("javascriptTextTransformFunctionName", "uppercaseName");

    // Act
    PipelineLauncher.LaunchInfo info = launchTemplate(options);
    assertThatPipeline(info).isRunning();

    // Construct a ChainedConditionCheck with 4 stages.
    // 1. Send initial wave of events to JDBC
    // 2. Wait on BigQuery to merge events from staging to destination
    // 3. Send wave of mutations to JDBC
    // 4. Wait on BigQuery to merge second wave of events
    Map<String, List<Map<String, Object>>> cdcEvents = new HashMap<>();
    ChainedConditionCheck conditionCheck =
        ChainedConditionCheck.builder(
                List.of(
                    writeJdbcData(tableName, cdcEvents),
                    BigQueryRowsCheck.builder(bigQueryResourceManager, bqTable)
                        .setMinRows(NUM_EVENTS)
                        .build(),
                    changeJdbcData(tableName, cdcEvents),
                    checkDestinationRows(tableName, cdcEvents)))
            .build();

    // Job needs to be cancelled as draining will time out
    PipelineOperator.Result result =
        pipelineOperator()
            .waitForConditionAndCancel(createConfig(info, Duration.ofMinutes(20)), conditionCheck);

    // Assert
    checkBigQueryTable(tableName, cdcEvents);
    assertThatResult(result).meetsConditions();
  }

  private JDBCResourceManager.JDBCSchema createMySqlSchema() {
    // Arrange MySQL-compatible schema
    HashMap<String, String> columns = new HashMap<>();
    columns.put(ROW_ID, "NUMERIC NOT NULL");
    columns.put(NAME, "VARCHAR(200)");
    columns.put(AGE, "NUMERIC");
    columns.put(MEMBER, "VARCHAR(200)");
    columns.put(ENTRY_ADDED, "VARCHAR(200)");
    return new JDBCResourceManager.JDBCSchema(columns, ROW_ID);
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method constructs the initial
   * rows of data in the JDBC database according to the common schema for the IT's in this class.
   *
   * @return A ConditionCheck containing the JDBC write operation.
   */
  private ConditionCheck writeJdbcData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected @NonNull String getDescription() {
        return "Send initial JDBC events.";
      }

      @Override
      protected @NonNull CheckResult check() {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          Map<String, Object> values = new HashMap<>();
          values.put(COLUMNS.get(0), i);
          values.put(COLUMNS.get(1), RandomStringUtils.randomAlphabetic(10).toLowerCase());
          values.put(COLUMNS.get(2), new Random().nextInt(100));
          values.put(COLUMNS.get(3), new Random().nextInt() % 2 == 0 ? "Y" : "N");
          values.put(COLUMNS.get(4), Instant.now().toString());
          rows.add(values);
        }

        cdcEvents.put(tableName, rows);
        return new CheckResult(
            cloudSqlResourceManager.write(tableName, rows),
            String.format("Sent %d rows to %s.", rows.size(), tableName));
      }
    };
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method changes rows of data in
   * the JDBC database according to the common schema for the IT's in this class. Half the rows are
   * mutated and half are removed completely.
   *
   * @return A ConditionCheck containing the JDBC mutate operation.
   */
  private ConditionCheck changeJdbcData(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected @NonNull String getDescription() {
        return "Send JDBC changes.";
      }

      @Override
      protected @NonNull CheckResult check() {
        List<Map<String, Object>> newCdcEvents = new ArrayList<>();
        for (int i = 0; i < NUM_EVENTS; i++) {
          if (i % 2 == 0) {
            Map<String, Object> values = cdcEvents.get(tableName).get(i);
            values.put(COLUMNS.get(1), values.get(COLUMNS.get(1)).toString().toUpperCase());
            values.put(COLUMNS.get(2), new Random().nextInt(100));
            values.put(
                COLUMNS.get(3),
                (Objects.equals(values.get(COLUMNS.get(3)).toString(), "Y") ? "N" : "Y"));

            String updateSql =
                "UPDATE "
                    + tableName
                    + " SET "
                    + COLUMNS.get(2)
                    + " = "
                    + values.get(COLUMNS.get(2))
                    + ","
                    + COLUMNS.get(3)
                    + " = '"
                    + values.get(COLUMNS.get(3))
                    + "'"
                    + " WHERE "
                    + COLUMNS.get(0)
                    + " = "
                    + i;
            cloudSqlResourceManager.runSQLUpdate(updateSql);
            newCdcEvents.add(values);
          } else {
            cloudSqlResourceManager.runSQLUpdate(
                "DELETE FROM " + tableName + " WHERE " + COLUMNS.get(0) + "=" + i);
          }
        }

        cdcEvents.put(tableName, newCdcEvents);
        return new CheckResult(
            true, String.format("Sent %d changes to %s.", newCdcEvents.size(), tableName));
      }
    };
  }

  /**
   * Helper function for constructing a ConditionCheck whose check() method checks the rows in the
   * destination BigQuery database for specific rows.
   *
   * @return A ConditionCheck containing the check operation.
   */
  private ConditionCheck checkDestinationRows(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {
    return new ConditionCheck() {
      @Override
      protected @NonNull String getDescription() {
        return "Check BigQuery rows.";
      }

      @Override
      protected @NonNull CheckResult check() {
        // First, check that correct number of rows were deleted.
        long totalRows = bigQueryResourceManager.getRowCount(tableName);
        long maxRows = cdcEvents.size();
        if (totalRows > maxRows) {
          return new CheckResult(
              false, String.format("Expected up to %d rows but found %d", maxRows, totalRows));
        }

        // Next, make sure in-place mutations were applied.
        try {
          checkBigQueryTable(tableName, cdcEvents);
          return new CheckResult(true, "BigQuery table contains expected rows.");
        } catch (AssertionError error) {
          return new CheckResult(false, "BigQuery table does not contain expected rows.");
        }
      }
    };
  }

  /** Helper function for checking the rows of the destination Spanner tables. */
  private void checkBigQueryTable(
      String tableName, Map<String, List<Map<String, Object>>> cdcEvents) {

    BigQueryAsserts.assertThatBigQueryRecords(bigQueryResourceManager.readTable(tableName))
        .hasRecordsUnorderedCaseInsensitiveColumns(cdcEvents.get(tableName));
  }

  private SubscriptionName createGcsNotifications() throws IOException {
    // Instantiate pubsub resource manager for notifications
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, PROJECT, credentialsProvider).build();

    // Create pubsub notifications
    TopicName topic = pubsubResourceManager.createTopic("it");
    TopicName dlqTopic = pubsubResourceManager.createTopic("dlq");
    SubscriptionName subscription = pubsubResourceManager.createSubscription(topic, "it-sub");

    gcsClient.createNotification(topic.toString(), gcsPrefix.substring(1));
    gcsClient.createNotification(dlqTopic.toString(), dlqGcsPrefix.substring(1));

    return subscription;
  }
}
