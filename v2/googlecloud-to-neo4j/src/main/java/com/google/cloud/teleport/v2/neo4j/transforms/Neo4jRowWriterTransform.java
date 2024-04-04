/*
 * Copyright (C) 2022 Google LLC
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
package com.google.cloud.teleport.v2.neo4j.transforms;

import com.google.cloud.teleport.v2.neo4j.database.CypherGenerator;
import com.google.cloud.teleport.v2.neo4j.database.Neo4jConnection;
import com.google.cloud.teleport.v2.neo4j.model.connection.ConnectionParams;
import com.google.cloud.teleport.v2.neo4j.model.helpers.TargetSequence;
import com.google.cloud.teleport.v2.neo4j.telemetry.Neo4jTelemetry;
import com.google.cloud.teleport.v2.neo4j.telemetry.ReportedSourceType;
import com.google.cloud.teleport.v2.neo4j.utils.DataCastingUtils;
import com.google.cloud.teleport.v2.neo4j.utils.SerializableSupplier;
import com.google.common.annotations.VisibleForTesting;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.importer.v1.Configuration;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.targets.CustomQueryTarget;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Neo4j write transformation. */
public class Neo4jRowWriterTransform extends PTransform<PCollection<Row>, PCollection<Row>> {
  private static final String NODE_BATCH_SIZE_SETTING = "node_target_batch_size";
  private static final String LEGACY_NODE_BATCH_SIZE_SETTING = "node_write_batch_size";
  private static final Integer DEFAULT_NODE_BATCH_SIZE = 5000;
  private static final String RELATIONSHIP_BATCH_SIZE_SETTING = "relationship_target_batch_size";
  private static final String LEGACY_RELATIONSHIP_BATCH_SIZE_SETTING = "edge_write_batch_size";
  private static final Integer DEFAULT_RELATIONSHIP_BATCH_SIZE = 1000;
  private static final String QUERY_BATCH_SIZE_SETTING = "query_target_batch_size";
  private static final String LEGACY_QUERY_BATCH_SIZE_SETTING = "custom_query_batch_size";
  private static final Integer DEFAULT_QUERY_BATCH_SIZE = 1000;

  private static final String NODE_PARALLELISM_SETTING = "node_target_parallelism";
  private static final String LEGACY_NODE_PARALLELISM_SETTING = "node_write_parallelism";
  private static final Integer DEFAULT_NODE_PARALLELISM_FACTOR = 5;
  private static final String RELATIONSHIP_PARALLELISM_SETTING = "relationship_target_parallelism";
  private static final String LEGACY_RELATIONSHIP_PARALLELISM_SETTING = "edge_write_parallelism";
  private static final Integer DEFAULT_RELATIONSHIP_PARALLELISM_FACTOR = 1;
  private static final String QUERY_PARALLELISM_SETTING = "query_target_parallelism";
  private static final String LEGACY_QUERY_PARALLELISM_SETTING = "custom_query_parallelism";
  private static final Integer DEFAULT_QUERY_PARALLELISM_FACTOR = 1;

  private static final Logger LOG = LoggerFactory.getLogger(Neo4jRowWriterTransform.class);
  private final ImportSpecification importSpecification;
  private final Target target;
  private final SerializableSupplier<Neo4jConnection> connectionSupplier;
  private final TargetSequence targetSequence;

  public Neo4jRowWriterTransform(
      ImportSpecification importSpecification,
      ConnectionParams neoConnection,
      String templateVersion,
      TargetSequence targetSequence,
      Target target) {
    this(
        importSpecification,
        targetSequence,
        target,
        () -> new Neo4jConnection(neoConnection, templateVersion));
  }

  @VisibleForTesting
  Neo4jRowWriterTransform(
      ImportSpecification importSpecification,
      TargetSequence targetSequence,
      Target target,
      SerializableSupplier<Neo4jConnection> connectionSupplier) {
    this.importSpecification = importSpecification;
    this.target = target;
    this.connectionSupplier = connectionSupplier;
    this.targetSequence = targetSequence;
  }

  @NonNull
  @Override
  public PCollection<Row> expand(@NonNull PCollection<Row> input) {
    var targetType = target.getTargetType();
    ReportedSourceType reportedSourceType = determineReportedSourceType();
    if (targetType == TargetType.NODE || targetType == TargetType.RELATIONSHIP) {
      createIndicesAndConstraints(reportedSourceType);
    }

    Configuration config = importSpecification.getConfiguration();

    Neo4jBlockingUnwindFn neo4jUnwindFn =
        new Neo4jBlockingUnwindFn(
            reportedSourceType,
            targetType,
            getCypherQuery(),
            false,
            "rows",
            getRowCastingFunction(),
            connectionSupplier);

    return input
        .apply("Create KV pairs", CreateKvTransform.of(parallelismFactor(targetType, config)))
        .apply("Group by keys", GroupByKey.create())
        .apply("Split into batches", ParDo.of(SplitIntoBatches.of(batchSize(targetType, config))))
        .apply(
            targetSequence.getSequenceNumber(target) + ": Neo4j write " + target.getName(),
            ParDo.of(neo4jUnwindFn))
        .setRowSchema(input.getSchema());
  }

  private ReportedSourceType determineReportedSourceType() {
    String sourceName = target.getSource();
    Source source = importSpecification.findSourceByName(sourceName);
    return ReportedSourceType.reportedSourceTypeOf(source);
  }

  private void createIndicesAndConstraints(ReportedSourceType reportedSourceType) {
    Set<String> cyphers = generateIndexAndConstraints();
    if (cyphers.isEmpty()) {
      return;
    }
    try (Neo4jConnection neo4jDirectConnect = connectionSupplier.get()) {
      LOG.info("Adding {} indices and constraints", cyphers.size());
      for (String cypher : cyphers) {
        LOG.info("Executing cypher: {}", cypher);
        try {
          TransactionConfig txConfig =
              TransactionConfig.builder()
                  .withMetadata(
                      Neo4jTelemetry.transactionMetadata(
                          Map.of(
                              "sink",
                              "neo4j",
                              "source",
                              reportedSourceType.format(),
                              "target-type",
                              target.getTargetType().name().toLowerCase(Locale.ROOT),
                              "step",
                              "init-schema")))
                  .build();
          neo4jDirectConnect.runAutocommit(cypher, txConfig);
        } catch (Exception e) {
          LOG.error("Error executing cypher: {}, {}", cypher, e.getMessage());
        }
      }
    }
  }

  private String getCypherQuery() {
    TargetType targetType = target.getTargetType();
    if (targetType == TargetType.QUERY) {
      String cypher = ((CustomQueryTarget) target).getQuery();
      LOG.info("Custom cypher query: {}", cypher);
      return cypher;
    }
    if (targetType != TargetType.NODE && targetType != TargetType.RELATIONSHIP) {
      throw new RuntimeException(String.format("Unsupported target type: %s", targetType));
    }
    String unwindCypher =
        CypherGenerator.getImportStatement(importSpecification, (EntityTarget) target);
    LOG.info("Unwind cypher: {}", unwindCypher);
    return unwindCypher;
  }

  private Set<String> generateIndexAndConstraints() {
    TargetType targetType = target.getTargetType();
    if (targetType != TargetType.NODE && targetType != TargetType.RELATIONSHIP) {
      throw new RuntimeException(String.format("Unsupported target type: %s", targetType));
    }
    return CypherGenerator.getSchemaStatements((EntityTarget) target);
  }

  private SerializableFunction<Row, Map<String, Object>> getRowCastingFunction() {
    return (row) -> DataCastingUtils.rowToNeo4jDataMap(row, target);
  }

  private static int batchSize(TargetType targetType, Configuration config) {
    switch (targetType) {
      case NODE:
        return config
            .get(Integer.class, NODE_BATCH_SIZE_SETTING, LEGACY_NODE_BATCH_SIZE_SETTING)
            .orElse(DEFAULT_NODE_BATCH_SIZE);
      case RELATIONSHIP:
        return config
            .get(
                Integer.class,
                RELATIONSHIP_BATCH_SIZE_SETTING,
                LEGACY_RELATIONSHIP_BATCH_SIZE_SETTING)
            .orElse(DEFAULT_RELATIONSHIP_BATCH_SIZE);
      case QUERY:
        return config
            .get(Integer.class, QUERY_BATCH_SIZE_SETTING, LEGACY_QUERY_BATCH_SIZE_SETTING)
            .orElse(DEFAULT_QUERY_BATCH_SIZE);
      default:
        throw new IllegalStateException(String.format("Unsupported target type: %s", targetType));
    }
  }

  private static int parallelismFactor(TargetType targetType, Configuration config) {
    switch (targetType) {
      case NODE:
        return config
            .get(Integer.class, NODE_PARALLELISM_SETTING, LEGACY_NODE_PARALLELISM_SETTING)
            .orElse(DEFAULT_NODE_PARALLELISM_FACTOR);
      case RELATIONSHIP:
        return config
            .get(
                Integer.class,
                RELATIONSHIP_PARALLELISM_SETTING,
                LEGACY_RELATIONSHIP_PARALLELISM_SETTING)
            .orElse(DEFAULT_RELATIONSHIP_PARALLELISM_FACTOR);
      case QUERY:
        return config
            .get(Integer.class, QUERY_PARALLELISM_SETTING, LEGACY_QUERY_PARALLELISM_SETTING)
            .orElse(DEFAULT_QUERY_PARALLELISM_FACTOR);
      default:
        throw new IllegalStateException(String.format("Unsupported target type: %s", targetType));
    }
  }
}
