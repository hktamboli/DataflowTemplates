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
package com.google.cloud.teleport.v2.neo4j.model.helpers;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.utils.FileSystemUtils;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.ImportSpecificationDeserializer;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.validation.SpecificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for parsing import specification files, accepts file URI as entry point. Delegates
 * to {@link ImportSpecificationDeserializer} for non-legacy spec payloads.
 */
public class JobSpecMapper {
  private static final Logger LOG = LoggerFactory.getLogger(JobSpecMapper.class);

  public static ImportSpecification parse(String jobSpecUri, OptionsParams options) {
    var json = fetchContent(jobSpecUri);
    var spec = new JSONObject(json);
    if (!spec.has("version")) {
      return parseLegacyJobSpec(options, spec);
    }
    try {
      // TODO: read query + input file pattern + runtime tokens for new spec
      return ImportSpecificationDeserializer.deserialize(new StringReader(json));
    } catch (SpecificationException e) {
      throw validationFailure(e);
    }
  }

  private static String fetchContent(String jobSpecUri) {
    try {
      return FileSystemUtils.getPathContents(jobSpecUri);
    } catch (Exception e) {
      LOG.error("Unable to fetch Neo4j job specification from URI {}: ", jobSpecUri, e);
      throw new RuntimeException(e);
    }
  }

  private static ImportSpecification parseLegacyJobSpec(OptionsParams options, JSONObject spec) {
    LOG.info("Converting legacy JSON job spec to new import specification format");
    var index = new JobSpecNameIndex();
    var targets = extractTargets(spec);
    TargetMapper.index(targets, index);
    var actions = extractActions(spec);
    ActionMapper.index(actions, index);
    var specification =
        new ImportSpecification(
            "0.legacy",
            parseConfig(spec),
            parseSources(spec, options),
            TargetMapper.parse(targets, options),
            ActionMapper.parse(actions, options));
    try {
      ImportSpecificationDeserializer.validate(specification);
    } catch (SpecificationException e) {
      throw validationFailure(e);
    }
    return specification;
  }

  private static RuntimeException validationFailure(SpecificationException e) {
    return new RuntimeException("Unable to process Neo4j job specification", e);
  }

  private static Map<String, Object> parseConfig(JSONObject json) {
    return json.has("config") ? json.getJSONObject("config").toMap() : null;
  }

  private static List<Source> parseSources(JSONObject json, OptionsParams options) {
    if (json.has("source")) {
      return List.of(SourceMapper.parse(json.getJSONObject("source"), options));
    }
    if (json.has("sources")) {
      return SourceMapper.parse(json.getJSONArray("sources"), options);
    }
    return List.of();
  }

  private static JSONArray extractTargets(JSONObject spec) {
    if (!spec.has("targets")) {
      throw new IllegalArgumentException("could not find any targets");
    }
    return spec.getJSONArray("targets");
  }

  private static JSONArray extractActions(JSONObject spec) {
    if (!spec.has("actions")) {
      return new JSONArray(0);
    }
    return spec.getJSONArray("actions");
  }
}
