/*
 * Copyright (C) 2019 Google LLC
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

import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_CHANGE_TYPE_KEY;
import static com.google.cloud.teleport.v2.templates.datastream.DatastreamConstants.EVENT_TABLE_NAME_KEY;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.teleport.v2.spanner.ddl.Ddl;
import com.google.cloud.teleport.v2.spanner.exceptions.TransformationException;
import com.google.cloud.teleport.v2.spanner.migrations.convertors.ChangeEventSessionConvertor;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.ChangeEventConvertorException;
import com.google.cloud.teleport.v2.spanner.migrations.exceptions.DroppedTableException;
import com.google.cloud.teleport.v2.spanner.migrations.schema.Schema;
import com.google.cloud.teleport.v2.spanner.migrations.transformation.TransformationContext;
import com.google.cloud.teleport.v2.spanner.migrations.utils.AdvancedTransformationImplFetcher;
import com.google.cloud.teleport.v2.spanner.utils.ISpannerMigrationTransformer;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationRequest;
import com.google.cloud.teleport.v2.spanner.utils.MigrationTransformationResponse;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContext;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventContextFactory;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventConvertor;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceFactory;
import com.google.cloud.teleport.v2.templates.datastream.InvalidChangeEventException;
import com.google.cloud.teleport.v2.values.FailsafeElement;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions;
import org.apache.beam.sdk.io.gcp.spanner.SpannerAccessor;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes Change events from DataStream into Cloud Spanner.
 *
 * <p>Change events are individually processed. Shadow tables store the version information(that
 * specifies the commit order) for each primary key. Shadow tables are consulted before actual
 * writes to Cloud Spanner to preserve the correctness and consistency of data.
 *
 * <p>Change events written successfully will be pushed onto the primary output with their commit
 * timestamps.
 *
 * <p>Change events that failed to be written will be pushed onto the secondary output tagged with
 * PERMANENT_ERROR_TAG/RETRYABLE_ERROR_TAG along with the exception that caused the failure.
 */
public class SpannerTransactionWriterDoFn extends DoFn<FailsafeElement<String, String>, Timestamp>
    implements Serializable {

  // TODO - Change Cloud Spanner nomenclature in code used to read DDL.

  private static final Logger LOG = LoggerFactory.getLogger(SpannerTransactionWriterDoFn.class);

  private final PCollectionView<Ddl> ddlView;

  // The mapping information read from the session file generated by HarbourBridge.
  private final Schema schema;

  /* The context used to populate transformation information */
  private final TransformationContext transformationContext;

  private final SpannerConfig spannerConfig;

  // The prefix for shadow tables.
  private final String shadowTablePrefix;

  // The source database type.
  private final String sourceType;

  // Jackson Object mapper.
  private transient ObjectMapper mapper;

  /* SpannerAccessor must be transient so that its value is not serialized at runtime. */
  private transient SpannerAccessor spannerAccessor;

  private final String customParameters;

  private final Counter processedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Total events processed");

  private final Counter successfulEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Successful events");

  private final Counter skippedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Skipped events");

  private final Counter failedEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Other permanent errors");

  private final Counter filteredEvents =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Filtered events");

  private final Counter conversionErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Conversion errors");

  private final Counter retryableErrors =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Retryable errors");

  private final Counter transformationException =
      Metrics.counter(SpannerTransactionWriterDoFn.class, "Transformation Exceptions");

  private final Distribution applyTransformationResponseTimeMetric =
      Metrics.distribution(
          SpannerTransactionWriterDoFn.class, "apply_transformation_impl_latency_ms");
  // The max length of tag allowed in Spanner Transaction tags.
  private static final int MAX_TXN_TAG_LENGTH = 50;

  /* The run mode, whether it is regular or retry. */
  private final Boolean isRegularRunMode;

  private final String customJarPath;

  private final String customClassName;

  private ISpannerMigrationTransformer datastreamToSpannerTransformation;

  // ChangeEventSessionConvertor utility object.
  private ChangeEventSessionConvertor changeEventSessionConvertor;

  public SpannerTransactionWriterDoFn(
      SpannerConfig spannerConfig,
      PCollectionView<Ddl> ddlView,
      Schema schema,
      TransformationContext transformationContext,
      String shadowTablePrefix,
      String sourceType,
      Boolean isRegularRunMode,
      String customParameters,
      String customJarPath,
      String customClassName) {
    Preconditions.checkNotNull(spannerConfig);
    this.spannerConfig = spannerConfig;
    this.ddlView = ddlView;
    this.schema = schema;
    this.transformationContext = transformationContext;
    this.shadowTablePrefix =
        (shadowTablePrefix.endsWith("_")) ? shadowTablePrefix : shadowTablePrefix + "_";
    this.sourceType = sourceType;
    this.isRegularRunMode = isRegularRunMode;
    this.customParameters = customParameters;
    this.customClassName = customClassName;
    this.customJarPath = customJarPath;
  }

  /** Setup function connects to Cloud Spanner. */
  @Setup
  public void setup() {
    spannerAccessor = SpannerAccessor.getOrCreate(spannerConfig);
    mapper = new ObjectMapper();
    mapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    datastreamToSpannerTransformation =
        AdvancedTransformationImplFetcher.getApplyTransformationImpl(
            customJarPath, customClassName, customParameters);
    changeEventSessionConvertor =
        new ChangeEventSessionConvertor(schema, transformationContext, sourceType);
  }

  /** Teardown function disconnects from the Cloud Spanner. */
  @Teardown
  public void teardown() {
    spannerAccessor.close();
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    FailsafeElement<String, String> msg = c.element();
    Ddl ddl = c.sideInput(ddlView);
    processedEvents.inc();

    boolean isRetryRecord = false;
    /*
     * Try Catch block to capture any exceptions that might occur while processing
     * DataStream events while writing to Cloud Spanner. All Exceptions that are caught
     * can be retried based on the exception type.
     */
    try {

      JsonNode changeEvent = mapper.readTree(msg.getPayload());

      JsonNode retryCount = changeEvent.get("_metadata_retry_count");
      String tableName = changeEvent.get(EVENT_TABLE_NAME_KEY).asText();
      Map<String, Object> sourceRecord = convertJsonNodeToMap(changeEvent);

      if (retryCount != null) {
        isRetryRecord = true;
      }

      if (!schema.isEmpty()) {
        schema.verifyTableInSession(changeEvent.get(EVENT_TABLE_NAME_KEY).asText());
        changeEvent = changeEventSessionConvertor.transformChangeEventViaSessionFile(changeEvent);
      }
      if (datastreamToSpannerTransformation != null) {

        Instant startTimestamp = Instant.now();
        MigrationTransformationRequest migrationTransformationRequest =
            new MigrationTransformationRequest(
                tableName, sourceRecord, "", changeEvent.get(EVENT_CHANGE_TYPE_KEY).asText());
        MigrationTransformationResponse migrationTransformationResponse =
            datastreamToSpannerTransformation.toSpannerRow(migrationTransformationRequest);
        Instant endTimestamp = Instant.now();
        applyTransformationResponseTimeMetric.update(
            new Duration(startTimestamp, endTimestamp).getMillis());
        if (migrationTransformationResponse.isEventFiltered()) {
          filteredEvents.inc();
          outputWithFilterTag(c, c.element());
          return;
        }
        changeEvent =
            transformChangeEventViaAdvancedTransformation(
                changeEvent, migrationTransformationResponse.getResponseRow());
      }
      ChangeEventContext changeEventContext =
          ChangeEventContextFactory.createChangeEventContext(
              changeEvent, ddl, shadowTablePrefix, sourceType);

      // Sequence information for the current change event.
      ChangeEventSequence currentChangeEventSequence =
          ChangeEventSequenceFactory.createChangeEventSequenceFromChangeEventContext(
              changeEventContext);

      // Start transaction
      spannerAccessor
          .getDatabaseClient()
          .readWriteTransaction(
              Options.tag(getTxnTag(c.getPipelineOptions())),
              Options.priority(spannerConfig.getRpcPriority().get()))
          .run(
              (TransactionCallable<Void>)
                  transaction -> {

                    // Sequence information for the last change event.
                    ChangeEventSequence previousChangeEventSequence =
                        ChangeEventSequenceFactory.createChangeEventSequenceFromShadowTable(
                            transaction, changeEventContext);

                    /* There was a previous event recorded with a greater sequence information
                     * than current. Hence, skip the current event.
                     */
                    if (previousChangeEventSequence != null
                        && previousChangeEventSequence.compareTo(currentChangeEventSequence) >= 0) {
                      return null;
                    }

                    // Apply shadow and data table mutations.
                    transaction.buffer(changeEventContext.getMutations());
                    return null;
                  });
      com.google.cloud.Timestamp timestamp = com.google.cloud.Timestamp.now();
      c.output(timestamp);
      successfulEvents.inc();

      // decrement the retry error count if this was retry attempt
      if (isRegularRunMode && isRetryRecord) {
        retryableErrors.dec();
      }

    } catch (DroppedTableException e) {
      // Errors when table exists in source but was dropped during conversion. We do not output any
      // errors to dlq for this.
      LOG.warn(e.getMessage());
      skippedEvents.inc();
    } catch (InvalidChangeEventException e) {
      // Errors that result from invalid change events.
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
      skippedEvents.inc();
    } catch (ChangeEventConvertorException e) {
      // Errors that result during Event conversions are not retryable.
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
      conversionErrors.inc();
    } catch (SpannerException | IllegalStateException ex) {
      /* Errors that happen when writing to Cloud Spanner are considered retryable.
       * Since all event conversion errors are caught beforehand as permanent errors,
       * any other errors encountered while writing to Cloud Spanner can be retried.
       * Examples include:
       * 1. Deadline exceeded errors from Cloud Spanner.
       * 2. Failures due to foreign key/interleaved table constraints.
       * 3. Any transient errors in Cloud Spanner.
       * IllegalStateException can occur due to conditions like spanner pool being closed,
       * in which case if this event is requed to same or different node at a later point in time,
       * a retry might work.
       */
      outputWithErrorTag(c, msg, ex, SpannerTransactionWriter.RETRYABLE_ERROR_TAG);
      // do not increment the retry error count if this was retry attempt
      if (!isRetryRecord) {
        retryableErrors.inc();
      }
    } catch (TransformationException e) {
      // Errors that result from the custom JAR during transformation are not retryable.
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
      transformationException.inc();
    } catch (Exception e) {
      // Any other errors are considered severe and not retryable.
      outputWithErrorTag(c, msg, e, SpannerTransactionWriter.PERMANENT_ERROR_TAG);
      failedEvents.inc();
    }
  }

  public JsonNode transformChangeEventViaAdvancedTransformation(
      JsonNode changeEvent, Map<String, Object> spannerRecord) throws IllegalArgumentException {
    for (Map.Entry<String, Object> entry : spannerRecord.entrySet()) {
      String columnName = entry.getKey();
      Object columnValue = entry.getValue();

      if (columnValue instanceof Boolean) {
        ((ObjectNode) changeEvent).put(columnName, (Boolean) columnValue);
      } else if (columnValue instanceof Long) {
        ((ObjectNode) changeEvent).put(columnName, (Long) columnValue);
      } else if (columnValue instanceof byte[]) {
        ((ObjectNode) changeEvent).put(columnName, (byte[]) columnValue);
      } else if (columnValue instanceof Double) {
        ((ObjectNode) changeEvent).put(columnName, (Double) columnValue);
      } else if (columnValue instanceof Integer) {
        ((ObjectNode) changeEvent).put(columnName, (Integer) columnValue);
      } else if (columnValue instanceof String) {
        ((ObjectNode) changeEvent).put(columnName, (String) columnValue);
      } else {
        throw new IllegalArgumentException(
            "Column name(" + columnName + ") has unsupported column value(" + columnValue + ")");
      }
    }
    return changeEvent;
  }

  public Map<String, Object> convertJsonNodeToMap(JsonNode changeEvent)
      throws InvalidChangeEventException {
    Map<String, Object> sourceRecord = new HashMap<>();
    List<String> changeEventColumns = ChangeEventConvertor.getEventColumnKeys(changeEvent);
    for (String key : changeEventColumns) {
      JsonNode value = changeEvent.get(key);
      if (value.isObject()) {
        sourceRecord.put(key, convertJsonNodeToMap(value));
      } else if (value.isArray()) {
        if (value.size() > 0 && value.get(0).isIntegralNumber()) {
          byte[] byteArray = new byte[value.size()];
          for (int i = 0; i < value.size(); i++) {
            byteArray[i] = (byte) value.get(i).intValue();
          }
          sourceRecord.put(key, byteArray);
        } else {
          throw new InvalidChangeEventException("Invalid byte array value for column: " + key);
        }
      } else if (value.isTextual()) {
        sourceRecord.put(key, value.asText());
      } else if (value.isBoolean()) {
        sourceRecord.put(key, value.asBoolean());
      } else if (value.isDouble() || value.isFloat() || value.isFloatingPointNumber()) {
        sourceRecord.put(key, value.asDouble());
      } else if (value.isLong() || value.isInt() || value.isIntegralNumber()) {
        sourceRecord.put(key, value.asLong());
      } else if (value.isNull()) {
        sourceRecord.put(key, null);
      }
    }
    return sourceRecord;
  }

  void outputWithErrorTag(
      ProcessContext c,
      FailsafeElement<String, String> changeEvent,
      Exception e,
      TupleTag<FailsafeElement<String, String>> errorTag) {
    // Making a copy, as the input must not be mutated.
    FailsafeElement<String, String> output = FailsafeElement.of(changeEvent);
    output.setErrorMessage(e.getMessage());
    c.output(errorTag, output);
  }

  void outputWithFilterTag(ProcessContext c, FailsafeElement<String, String> changeEvent) {
    c.output(SpannerTransactionWriter.FILTERED_EVENT_TAG, changeEvent.getPayload());
  }

  String getTxnTag(PipelineOptions options) {
    String jobId = "datastreamToSpanner";
    try {
      DataflowWorkerHarnessOptions harnessOptions = options.as(DataflowWorkerHarnessOptions.class);
      jobId = harnessOptions.getJobId();
    } catch (Exception e) {
      LOG.warn(
          "Unable to find Dataflow job id. Spanner transaction tags will not contain the dataflow"
              + " job id.",
          e);
    }
    // Spanner transaction tags have a limit of 50 characters. Dataflow job id is 40 chars in
    // length.
    String txnTag = "txBy=" + jobId;
    if (txnTag.length() > MAX_TXN_TAG_LENGTH) {
      txnTag = txnTag.substring(0, MAX_TXN_TAG_LENGTH);
    }
    return txnTag;
  }
}
