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
package com.google.cloud.teleport.templates.common;

import com.google.api.client.util.DateTime;
import com.google.cloud.teleport.coders.FailsafeElementCoder;
import com.google.cloud.teleport.sentinel.SentinelEvent;
import com.google.cloud.teleport.sentinel.SentinelEventCoder;
import com.google.cloud.teleport.values.FailsafeElement;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Unit tests for {@link com.google.cloud.teleport.templates.common.SentinelConverters} class. */
public class SentinelConvertersTest {

  private static final TupleTag<SentinelEvent> Sentinel_EVENT_OUT = new TupleTag<SentinelEvent>() {};
  private static final TupleTag<FailsafeElement<String, String>> Sentinel_EVENT_DEADLETTER_OUT =
      new TupleTag<FailsafeElement<String, String>>() {};
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static final Gson GSON = new Gson();

  /** Test successful conversion of simple String payloads. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventSimpleStrings() {

    FailsafeElement<String, String> hello = FailsafeElement.of("hello", "hello");
    FailsafeElement<String, String> world = FailsafeElement.of("world", "world");

    pipeline.getCoderRegistry().registerCoderForClass(SentinelEvent.class, SentinelEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(hello, world)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SentinelConverters.failsafeStringToSentinelEvent(
                    Sentinel_EVENT_OUT, Sentinel_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(Sentinel_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(Sentinel_EVENT_OUT))
        .containsInAnyOrder(
            SentinelEvent.newBuilder().withEvent("hello").build(),
            SentinelEvent.newBuilder().withEvent("world").build());

    pipeline.run();
  }

  /** Test successful conversion of invalid JSON messages. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventInvalidJSON() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "" + "\t\"name\": \"Jim\",\n" + "}", "{\n" + "\t\"name\": \"Jim\",\n" + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder().withEvent("{\n" + "\t\"name\": \"Jim\",\n" + "}").build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventValidJSON() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "" + "\t\"name\": \"Jim\",\n" + "}", "{\n" + "\t\"name\": \"Jim\"\n" + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder().withEvent("{\n" + "\t\"name\": \"Jim\"\n" + "}").build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with a valid timestamp. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventValidTimestamp() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"logName\": \"test-log-name\",\n"
                + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\"\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder()
            .withEvent(
                "{\n"
                    + "\t\"name\": \"Jim\",\n"
                    + "\t\"logName\": \"test-log-name\",\n"
                    + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\"\n"
                    + "}")
            .withTime(DateTime.parseRfc3339("2019-10-15T11:32:26.553Z").getValue())
            .build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with an invalid timestamp. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventInValidTimestamp() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"logName\": \"test-log-name\",\n"
                + "\t\"timestamp\": \"2019-1011:32:26.553Z\"\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder()
            .withEvent(
                "{\n"
                    + "\t\"name\": \"Jim\",\n"
                    + "\t\"logName\": \"test-log-name\",\n"
                    + "\t\"timestamp\": \"2019-1011:32:26.553Z\"\n"
                    + "}")
            .build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with a user provided _metadata. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventValidSource() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"source\": \"test-log-name\"}\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder()
            .withEvent("{\"name\":\"Jim\"}")
            .withSource("test-log-name")
            .build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with a user provided host. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventValidHost() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"host\": \"test-host\"}\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder().withEvent("{\"name\":\"Jim\"}").withHost("test-host").build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with a user provided index. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventValidIndex() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"host\": \"test-host\","
                + "\"index\":\"test-index\"}\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder()
            .withEvent("{\"name\":\"Jim\"}")
            .withHost("test-host")
            .withIndex("test-index")
            .build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with user provided index 'fields'. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEvent_withValidFields() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"fields\":{\"test-key\":\"test-value\"}}\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder()
            .withEvent("{\"name\":\"Jim\"}")
            .withFields(GSON.fromJson("{\"test-key\":\"test-value\"}", JsonObject.class))
            .build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with user provided empty index 'fields'. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEvent_withEmptyFields() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "", "{\n" + "\t\"name\": \"Jim\",\n" + "\t\"_metadata\": {\"fields\":{}}\n" + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder()
            .withEvent("{\"name\":\"Jim\"}")
            .withFields(GSON.fromJson("{}", JsonObject.class))
            .build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with user provided invalid index 'fields'. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEvent_withInvalidFields() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"name\": \"Jim\",\n"
                + "\t\"_metadata\": {\"fields\":\"invalid-json-fields\"}\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder().withEvent("{\"name\":\"Jim\"}").build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of JSON messages with provided overrides for time and source. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventValidTimeOverride() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "\t\"timestamp\": \"2019-10-15T11:32:26.553Z\",\n"
                + "\t\"_metadata\": {\"time\": \"2019-11-22T11:32:26.553Z\", "
                + "\"source\": \"test-source-name\"}\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder()
            .withEvent("{" + "\"timestamp\":\"2019-10-15T11:32:26.553Z\"" + "}")
            .withSource("test-source-name")
            .withTime(DateTime.parseRfc3339("2019-11-22T11:32:26.553Z").getValue())
            .build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  /** Test successful conversion of Pub/Sub messages with provided override for time. */
  @Test
  @Category(NeedsRunner.class)
  public void testFailsafeStringToSentinelEventPubsubMessageTimeOverride() {

    FailsafeElement<String, String> input =
        FailsafeElement.of(
            "",
            "{\n"
                + "  \"data\": {\n"
                + "    \"timestamp\": \"2021-04-16T14:10:04.634492Z\"\n"
                + "  },\n"
                + "  \"attributes\": {\n"
                + "    \"test-key\": \"test-value\"\n"
                + "  }\n"
                + "}");

    SentinelEvent expectedSentinelEvent =
        SentinelEvent.newBuilder()
            .withEvent(
                "{\n"
                    + "  \"data\": {\n"
                    + "    \"timestamp\": \"2021-04-16T14:10:04.634492Z\"\n"
                    + "  },\n"
                    + "  \"attributes\": {\n"
                    + "    \"test-key\": \"test-value\"\n"
                    + "  }\n"
                    + "}")
            .withTime(DateTime.parseRfc3339("2021-04-16T14:10:04.634492Z").getValue())
            .build();

    matchesSentinelEvent(input, expectedSentinelEvent);
  }

  @Category(NeedsRunner.class)
  private void matchesSentinelEvent(
      FailsafeElement<String, String> input, SentinelEvent expectedSentinelEvent) {
    pipeline.getCoderRegistry().registerCoderForClass(SentinelEvent.class, SentinelEventCoder.of());

    PCollectionTuple tuple =
        pipeline
            .apply(
                Create.of(input)
                    .withCoder(FailsafeElementCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())))
            .apply(
                SentinelConverters.failsafeStringToSentinelEvent(
                    Sentinel_EVENT_OUT, Sentinel_EVENT_DEADLETTER_OUT));

    PAssert.that(tuple.get(Sentinel_EVENT_DEADLETTER_OUT)).empty();
    PAssert.that(tuple.get(Sentinel_EVENT_OUT)).containsInAnyOrder(expectedSentinelEvent);

    pipeline.run();
  }
}
