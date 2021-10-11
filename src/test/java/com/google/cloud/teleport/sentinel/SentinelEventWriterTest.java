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
package com.google.cloud.teleport.sentinel;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assume.assumeNoException;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

/** Unit tests for {@link com.google.cloud.teleport.sentinel.SentinelEventWriter} class. */
public class SentinelEventWriterTest {

  private static final String EXPECTED_PATH = "/";

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  // We create a MockServerRule to simulate an actual Sentinel server.
  @Rule public MockServerRule mockServerRule;
  private MockServerClient mockServerClient;

  @Before
  public void setup() {
    try {
      mockServerRule = new MockServerRule(this);
    } catch (Exception e) {
      assumeNoException(e);
    }
  }

  /** Test building {@link SentinelEventWriter} with missing URL. */
  @Test
  public void eventWriterMissingURL() {

    Exception thrown =
        assertThrows(NullPointerException.class, () -> SentinelEventWriter.newBuilder().build());

    assertThat(thrown).hasMessageThat().contains("url needs to be provided");
  }

  /** Test building {@link SentinelEventWriter} with missing URL protocol. */
  @Test
  public void eventWriterMissingURLProtocol() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> SentinelEventWriter.newBuilder().withUrl("test-url").build());

    assertThat(thrown).hasMessageThat().contains(SentinelEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /** Test building {@link SentinelEventWriter} with an invalid URL. */
  @Test
  public void eventWriterInvalidURL() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> SentinelEventWriter.newBuilder().withUrl("http://1.2.3").build());

    assertThat(thrown).hasMessageThat().contains(SentinelEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /**
   * Test building {@link SentinelEventWriter} with the 'services/collector/event' path appended to
   * the URL.
   */
  @Test
  public void eventWriterFullEndpoint() {

    Exception thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SentinelEventWriter.newBuilder()
                    .withUrl("http://test-url:8088/services/collector/event")
                    .build());

    assertThat(thrown).hasMessageThat().contains(SentinelEventWriter.INVALID_URL_FORMAT_MESSAGE);
  }

  /** Test building {@link SentinelEventWriter} with missing token. */
  @Test
  public void eventWriterMissingToken() {

    Exception thrown =
        assertThrows(
            NullPointerException.class,
            () -> SentinelEventWriter.newBuilder().withUrl("http://test-url").build());

    assertThat(thrown).hasMessageThat().contains("token needs to be provided");
  }

  /**
   * Test building {@link SentinelEventWriter} with default batchcount and certificate validation .
   */
  @Test
  public void eventWriterDefaultBatchCountAndValidation() {

    SentinelEventWriter writer =
        SentinelEventWriter.newBuilder()
            .withUrl("http://test-url")
            .withToken("test-token")
            .withCustomerId("test-customerId")
            .withLogTableName("test-logTableName")
            .build();

    assertThat(writer.inputBatchCount()).isNull();
    assertThat(writer.disableCertificateValidation()).isNull();
  }

  /** Test building {@link SentinelEventWriter} with custom batchcount and certificate validation . */
  @Test
  public void eventWriterCustomBatchCountAndValidation() {

    Integer batchCount = 30;
    Boolean certificateValidation = false;
    SentinelEventWriter writer =
        SentinelEventWriter.newBuilder()
            .withUrl("http://test-url")
            .withToken("test-token")
            .withCustomerId("test-customerId")
            .withLogTableName("test-logTableName")
            .withInputBatchCount(StaticValueProvider.of(batchCount))
            .withDisableCertificateValidation(StaticValueProvider.of(certificateValidation))
            .build();

    assertThat(writer.inputBatchCount().get()).isEqualTo(batchCount);
    assertThat(writer.disableCertificateValidation().get()).isEqualTo(certificateValidation);
  }

  /** Test successful POST request for single batch. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulSentinelWriteSingleBatchTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServerRule.getPort();

    List<KV<Integer, SentinelEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                SentinelEvent.newBuilder()
                    .withEvent("test-event-1")
                    .withHost("test-host-1")
                    .withIndex("test-index-1")
                    .withSource("test-source-1")
                    .withSourceType("test-source-type-1")
                    .withTime(12345L)
                    .build()),
            KV.of(
                123,
                SentinelEvent.newBuilder()
                    .withEvent("test-event-2")
                    .withHost("test-host-2")
                    .withIndex("test-index-2")
                    .withSource("test-source-2")
                    .withSourceType("test-source-type-2")
                    .withTime(12345L)
                    .build()));

    PCollection<SentinelWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), SentinelEventCoder.of())))
            .apply(
                "SentinelEventWriter",
                ParDo.of(
                    SentinelEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(1)) // Test one request per SentinelEvent
                        .withToken("test-token")
                        .withCustomerId("test-customerId")
                        .withLogTableName("test-logTableName")            
                        .build()))
            .setCoder(SentinelWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly the expected number of POST requests.
    mockServerClient.verify(
        HttpRequest.request(EXPECTED_PATH), VerificationTimes.exactly(testEvents.size()));
  }

  /** Test successful POST request for multi batch. */
  @Test
  @Category(NeedsRunner.class)
  public void successfulSentinelWriteMultiBatchTest() {

    // Create server expectation for success.
    mockServerListening(200);

    int testPort = mockServerRule.getPort();

    List<KV<Integer, SentinelEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                SentinelEvent.newBuilder()
                    .withEvent("test-event-1")
                    .withHost("test-host-1")
                    .withIndex("test-index-1")
                    .withSource("test-source-1")
                    .withSourceType("test-source-type-1")
                    .withTime(12345L)
                    .build()),
            KV.of(
                123,
                SentinelEvent.newBuilder()
                    .withEvent("test-event-2")
                    .withHost("test-host-2")
                    .withIndex("test-index-2")
                    .withSource("test-source-2")
                    .withSourceType("test-source-type-2")
                    .withTime(12345L)
                    .build()));

    PCollection<SentinelWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), SentinelEventCoder.of())))
            .apply(
                "SentinelEventWriter",
                ParDo.of(
                    SentinelEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(
                                testEvents.size())) // all requests in a single batch.
                        .withCustomerId("test-customerId")
                        .withLogTableName("test-logTableName")                    
                        .withToken("test-token")
                        .build()))
            .setCoder(SentinelWriteErrorCoder.of());

    // All successful responses.
    PAssert.that(actual).empty();

    pipeline.run();

    // Server received exactly one POST request.
    mockServerClient.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  /** Test failed POST request. */
  @Test
  @Category(NeedsRunner.class)
  public void failedSentinelWriteSingleBatchTest() {

    // Create server expectation for FAILURE.
    mockServerListening(404);

    int testPort = mockServerRule.getPort();

    List<KV<Integer, SentinelEvent>> testEvents =
        ImmutableList.of(
            KV.of(
                123,
                SentinelEvent.newBuilder()
                    .withEvent("test-event-1")
                    .withHost("test-host-1")
                    .withIndex("test-index-1")
                    .withSource("test-source-1")
                    .withSourceType("test-source-type-1")
                    .withTime(12345L)
                    .build()));

    PCollection<SentinelWriteError> actual =
        pipeline
            .apply(
                "Create Input data",
                Create.of(testEvents)
                    .withCoder(KvCoder.of(BigEndianIntegerCoder.of(), SentinelEventCoder.of())))
            .apply(
                "SentinelEventWriter",
                ParDo.of(
                    SentinelEventWriter.newBuilder()
                        .withUrl(Joiner.on(':').join("http://localhost", testPort))
                        .withInputBatchCount(
                            StaticValueProvider.of(
                                testEvents.size())) // all requests in a single batch.
                        .withCustomerId("test-customerId")
                        .withLogTableName("test-logTableName")                    
                        .withToken("test-token")
                        .build()))
            .setCoder(SentinelWriteErrorCoder.of());

    // Expect a single 404 Not found SentinelWriteError
    PAssert.that(actual)
        .containsInAnyOrder(
            SentinelWriteError.newBuilder()
                .withStatusCode(404)
                .withStatusMessage("Not Found")
                .withPayload(
                    "{\"time\":12345,\"host\":\"test-host-1\","
                        + "\"source\":\"test-source-1\",\"sourcetype\":\"test-source-type-1\","
                        + "\"index\":\"test-index-1\",\"event\":\"test-event-1\"}")
                .build());

    pipeline.run();

    // Server received exactly one POST request.
    mockServerClient.verify(HttpRequest.request(EXPECTED_PATH), VerificationTimes.once());
  }

  private void mockServerListening(int statusCode) {
    try {
      mockServerClient
          .when(HttpRequest.request(EXPECTED_PATH))
          .respond(HttpResponse.response().withStatusCode(statusCode));
    } catch (Exception e) {
      assumeNoException(e);
    }
  }
}
