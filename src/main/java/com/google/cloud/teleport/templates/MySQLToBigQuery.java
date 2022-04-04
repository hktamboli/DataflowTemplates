/*
 * Copyright (C) 2018 Google LLC
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
package com.google.cloud.teleport.templates;

import com.google.api.services.bigquery.model.TableRow;
import com.google.auto.service.AutoService;
import com.google.cloud.teleport.io.DynamicJdbcIO;
import com.google.cloud.teleport.templates.common.JdbcConverters;
import com.google.cloud.teleport.util.KMSEncryptedNestedValueProvider;
import java.security.Security;
import javax.net.ssl.SSLServerSocketFactory;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A template that copies data from a relational database using JDBC to an existing BigQuery table.
 */
public class JdbcToBigQuery {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcToBigQuery.class);

  /**
   * Custom JvmInitializer to override jdk.tls.disabledAlgorithms through the template parameters.
   */
  @AutoService(JvmInitializer.class)
  public static class CustomJvmInitializer implements JvmInitializer {
    @Override
    public void onStartup() {}

    @Override
    public void beforeProcessing(PipelineOptions options) {
      JdbcConverters.JdbcToBigQueryOptions pipelineOptions =
          options.as(JdbcConverters.JdbcToBigQueryOptions.class);
      if (pipelineOptions.getDisabledAlgorithms() != null
          && pipelineOptions.getDisabledAlgorithms().get() != null) {
        String value = pipelineOptions.getDisabledAlgorithms().get();
        // if the user sets disabledAlgorithms to "none" then set "jdk.tls.disabledAlgorithms" to ""
        if (value.equals("none")) {
          value = "";
        }
        LOG.info("disabledAlgorithms is set to {}.", value);
        Security.setProperty("jdk.tls.disabledAlgorithms", value);
        SSLServerSocketFactory fact = (SSLServerSocketFactory) SSLServerSocketFactory.getDefault();
        LOG.info("Supported Ciper Suites: " + String.join("\n", fact.getSupportedCipherSuites()));
      }
    }
  }

  private static ValueProvider<String> maybeDecrypt(
      ValueProvider<String> unencryptedValue, ValueProvider<String> kmsKey) {
    return new KMSEncryptedNestedValueProvider(unencryptedValue, kmsKey);
  }

  /**
   * Main entry point for executing the pipeline. This will run the pipeline asynchronously. If
   * blocking execution is required, use the {@link
   * JdbcToBigQuery#run(JdbcConverters.JdbcToBigQueryOptions)} method to start the pipeline and
   * invoke {@code result.waitUntilFinish()} on the {@link PipelineResult}
   *
   * @param args The command-line arguments to the pipeline.
   */
  public static void main(String[] args) {

    // Parse the user options passed from the command-line
    JdbcConverters.JdbcToBigQueryOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(JdbcConverters.JdbcToBigQueryOptions.class);

    run(options);
  }

  /**
   * Runs the pipeline with the supplied options.
   *
   * @param options The execution parameters to the pipeline.
   * @return The result of the pipeline execution.
   */
  private static PipelineResult run(JdbcConverters.JdbcToBigQueryOptions options) {
    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);

    /*
     * Steps: 1) Read records via JDBC and convert to TableRow via RowMapper
     *        2) Append TableRow to BigQuery via BigQueryIO
     */
    pipeline
        /*
         * Step 1: Read records via JDBC and convert to TableRow
         *         via {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper}
         */
        .apply(
            "Read from MySQL",
            DynamicJdbcIO.<TableRow>read()
                .withDataSourceConfiguration(
                    DynamicJdbcIO.DynamicDataSourceConfiguration.create(
                            options.getDriverClassName(),
                            maybeDecrypt(options.getConnectionURL(), options.getKMSEncryptionKey()))
                        .withUsername(
                            maybeDecrypt(options.getUsername(), options.getKMSEncryptionKey()))
                        .withPassword(
                            maybeDecrypt(options.getPassword(), options.getKMSEncryptionKey()))
                        .withDriverJars(options.getDriverJars())
                        .withConnectionProperties(options.getConnectionProperties()))
                .withQuery(options.getQuery())
                .withCoder(TableRowJsonCoder.of())
                .withRowMapper(new ResultSetToTableRow(options.getTimezone())))
        /*
         * Step 2: Append TableRow to an existing BigQuery table
         */
        .apply(
            "Write to BigQuery",
            BigQueryIO.writeTableRows()
                .withoutValidation()
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                .withCustomGcsTempLocation(options.getBigQueryLoadingTemporaryDirectory())
                .withSchema(
                    NestedValueProvider.of(
                        options.getSchema(),
                        new SerializableFunction<String, TableSchema>() {

                          @Override
                          public TableSchema apply(String jsonPath) {

                            TableSchema tableSchema = new TableSchema();
                            List<TableFieldSchema> fields = new ArrayList<>();
                            SchemaParser schemaParser = new SchemaParser();

                            try {
                              JSONArray bqSchemaJsonArray = schemaParser.parseSchema(jsonPath);
                              for (int i = 0; i < bqSchemaJsonArray.length(); i++) {
                                JSONObject inputField = bqSchemaJsonArray.getJSONObject(i);
                                TableFieldSchema field =
                                    new TableFieldSchema()
                                        .setName(inputField.getString(NAME))
                                        .setType(inputField.getString(TYPE));

                                if (inputField.has(MODE)) {
                                  field.setMode(inputField.getString(MODE));
                                }

                                fields.add(field);
                              }
                              tableSchema.setFields(fields);

                            } catch (Exception e) {
                              throw new RuntimeException(e);
                            }
                            return tableSchema;
                          }
                        }))
                .to(options.getOutputTable()));

    // Execute the pipeline and return the result.
    return pipeline.run();
  }

  /**
   * {@link JdbcIO.RowMapper} implementation to convert Jdbc ResultSet rows to UTF-8 encoded JSONs.
   */
  private static class ResultSetToTableRow implements JdbcIO.RowMapper<TableRow> {

    private final String timezone;

    ResultSetToTableRow(ValueProvider<String> timezone) {
      this.timezone = timezone.get();
    }

    @Override
    public TableRow mapRow(ResultSet resultSet) throws Exception {

      ResultSetMetaData metaData = resultSet.getMetaData();
      SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
      // ref: https://docs.oracle.com/javase/jp/8/docs/api/java/time/format/DateTimeFormatter.html
      DateTimeFormatter datetimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

      Logger LOG = LoggerFactory.getLogger(SQLServerToBigQuery.class);
      TableRow outputTableRow = new TableRow();

      for (int i = 1; i <= metaData.getColumnCount(); i++) {
        if (resultSet.getObject(i) == null) {
          outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
          continue;
        }

        switch (metaData.getColumnTypeName(i).toLowerCase()) {
          case "date":
            outputTableRow.set(
                metaData.getColumnName(i), dateFormatter.format(resultSet.getObject(i)));
            break;
          case "datetime":
            outputTableRow.set(
              metaData.getColumnName(i),
              datetimeFormatter.format((TemporalAccessor) resultSet.getObject(i)));
            break;
          default:
            outputTableRow.set(metaData.getColumnName(i), resultSet.getObject(i));
        }
      }
      return outputTableRow;
    }
  }
}
