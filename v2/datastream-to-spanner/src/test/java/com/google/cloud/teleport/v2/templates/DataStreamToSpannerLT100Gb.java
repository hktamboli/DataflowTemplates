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

import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.teleport.metadata.TemplateLoadTest;
import com.google.pubsub.v1.SubscriptionName;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.it.common.PipelineLauncher;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineOperator;
import org.apache.beam.it.common.TestProperties;
import org.apache.beam.it.common.utils.ResourceManagerUtils;
import org.apache.beam.it.conditions.ChainedConditionCheck;
import org.apache.beam.it.conditions.ConditionCheck;
import org.apache.beam.it.gcp.datastream.DatastreamResourceManager;
import org.apache.beam.it.gcp.datastream.MySQLSource;
import org.apache.beam.it.gcp.pubsub.PubsubResourceManager;
import org.apache.beam.it.gcp.secretmanager.SecretManagerResourceManager;
import org.apache.beam.it.gcp.spanner.SpannerResourceManager;
import org.apache.beam.it.gcp.spanner.conditions.SpannerRowsCheck;
import org.apache.beam.it.gcp.storage.GcsResourceManager;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Performance tests for {@link DataStreamToSpanner} DataStream to Spanner template. */
@Category(TemplateLoadTest.class)
@TemplateLoadTest(DataStreamToSpanner.class)
@RunWith(JUnit4.class)
public class DataStreamToSpannerLT100Gb extends DataStreamToSpannerLTBase {

  private static final String SPEC_PATH =
      "gs://dataflow-templates/latest/flex/Cloud_Datastream_to_Spanner";
  private static final String ARTIFACT_BUCKET = TestProperties.artifactBucket();
  private static final String TEST_ROOT_DIR = DataStreamToSpannerLT100Gb.class.getSimpleName();
  private static final String SPANNER_DDL_RESOURCE =
      "DataStreamToSpannerLT100Gb/spanner-schema.sql";
  private static final String TABLE = "person";
  private static final int MAX_WORKERS = 100;
  private static final int NUM_WORKERS = 50;
  public static PubsubResourceManager pubsubResourceManager;
  public static SpannerResourceManager spannerResourceManager;
  private static GcsResourceManager gcsResourceManager;
  public static DatastreamResourceManager datastreamResourceManager;
  private SecretManagerResourceManager secretClient;

  /**
   * Setup resource managers.
   *
   * @throws IOException
   */
  @Before
  public void setUpResourceManagers() throws IOException {
    spannerResourceManager =
        SpannerResourceManager.builder(testName, project, region)
            .maybeUseStaticInstance()
            .setNodeCount(10)
            .build();
    pubsubResourceManager =
        PubsubResourceManager.builder(testName, project, CREDENTIALS_PROVIDER).build();

    gcsResourceManager =
        GcsResourceManager.builder(ARTIFACT_BUCKET, TEST_ROOT_DIR, CREDENTIALS).build();
    datastreamResourceManager =
        DatastreamResourceManager.builder(testName, project, region)
            .setCredentialsProvider(CREDENTIALS_PROVIDER)
            .build();
    secretClient = SecretManagerResourceManager.builder(project, CREDENTIALS_PROVIDER).build();
  }

  /**
   * Cleanup resource managers.
   *
   * @throws IOException
   */
  @AfterClass
  public static void cleanUp() throws IOException {
    ResourceManagerUtils.cleanResources(
        spannerResourceManager,
        pubsubResourceManager,
        gcsResourceManager,
        datastreamResourceManager);
  }

  @Test
  public void backfill100Gb() throws IOException, ParseException, InterruptedException {
    // Setup resources
    createSpannerDDL(spannerResourceManager, SPANNER_DDL_RESOURCE);

    // TestClassName/runId/TestMethodName/cdc
    String gcsPrefix =
        String.join("/", new String[] {TEST_ROOT_DIR, gcsResourceManager.runId(), testName, "cdc"});
    SubscriptionName subscription =
        createPubsubResources(
            TEST_ROOT_DIR + testName, pubsubResourceManager, gcsPrefix, gcsResourceManager);

    String dlqGcsPrefix =
        String.join("/", new String[] {TEST_ROOT_DIR, gcsResourceManager.runId(), testName, "dlq"});
    SubscriptionName dlqSubscription =
        createPubsubResources(
            TEST_ROOT_DIR + testName + "dlq",
            pubsubResourceManager,
            dlqGcsPrefix,
            gcsResourceManager);

    // Setup Datastream
    MySQLSource mySQLSource = getMySQLSource();
    Stream stream =
        createDatastreamResources(
            ARTIFACT_BUCKET, gcsPrefix, mySQLSource, datastreamResourceManager);

    // Setup Parameters
    Map<String, String> params =
        new HashMap<>() {
          {
            put("inputFilePattern", getGcsPath(ARTIFACT_BUCKET, gcsPrefix));
            put("streamName", stream.getName());
            put("instanceId", spannerResourceManager.getInstanceId());
            put("databaseId", spannerResourceManager.getDatabaseId());
            put("projectId", project);
            put("deadLetterQueueDirectory", getGcsPath(ARTIFACT_BUCKET, dlqGcsPrefix));
            put("gcsPubSubSubscription", subscription.toString());
            put("dlqGcsPubSubSubscription", dlqSubscription.toString());
            put("datastreamSourceType", "mysql");
            put("inputFileFormat", "avro");
          }
        };

    LaunchConfig.Builder options = LaunchConfig.builder(getClass().getSimpleName(), SPEC_PATH);
    options.addEnvironment("maxWorkers", MAX_WORKERS).addEnvironment("numWorkers", NUM_WORKERS);

    options.setParameters(params);

    // Act
    PipelineLauncher.LaunchInfo jobInfo = pipelineLauncher.launch(project, region, options.build());
    assertThatPipeline(jobInfo).isRunning();

    List<ConditionCheck> checks = new ArrayList<>();
    for (int i = 1; i <= 10; ++i) {
      checks.add(
          SpannerRowsCheck.builder(spannerResourceManager, TABLE + i)
              .setMinRows(6500000)
              .setMaxRows(6500000)
              .build());
    }

    ChainedConditionCheck conditionCheck = ChainedConditionCheck.builder(checks).build();

    PipelineOperator.Result result =
        pipelineOperator.waitForCondition(
            createConfig(jobInfo, Duration.ofHours(4), Duration.ofMinutes(5)), conditionCheck);

    // Assert Conditions
    assertThatResult(result).meetsConditions();

    result = pipelineOperator.cancelJobAndFinish(createConfig(jobInfo, Duration.ofMinutes(5)));
    assertThatResult(result).isLaunchFinished();

    // export results
    exportMetricsToBigQuery(jobInfo, getMetrics(jobInfo));
  }

  public MySQLSource getMySQLSource() {
    String password =
        secretClient.accessSecret(
            "projects/269744978479/secrets/nokill-datastream-mysql-to-spanner-password/versions/1");
    MySQLSource mySQLSource =
        new MySQLSource.Builder("34.41.46.219", "root", password, 3306).build();
    return mySQLSource;
  }
}
