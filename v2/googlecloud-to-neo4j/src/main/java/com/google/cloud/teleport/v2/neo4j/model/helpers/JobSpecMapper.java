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
import org.json.JSONObject;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.ImportSpecificationDeserializer;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.targets.Targets;
import org.neo4j.importer.v1.validation.SpecificationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for parsing import specification files, accepts file URI as entry point. Delegates
 * to {@link ImportSpecificationDeserializer} for non-legacy spec payloads.
 */
public class JobSpecMapper {
  private static final Logger LOG = LoggerFactory.getLogger(JobSpecMapper.class);

  public static ImportSpecification fromUri(String jobSpecUri, OptionsParams options) {
    String rawJson = fetchContent(jobSpecUri);
    var json = new JSONObject(rawJson);
    if (json.has("version")) {
      try {
        // TODO: read query + input file pattern + runtime tokens for new spec
        return ImportSpecificationDeserializer.deserialize(new StringReader(rawJson));
      } catch (SpecificationException e) {
        throw new RuntimeException("Unable to parse Neo4j job specification", e);
      }
    }
    // legacy JSON conversion to new specification
    Map<String, Object> config = json.has("config") ? json.getJSONObject("config").toMap() : null;
    List<Source> sources = parseSources(json, options);
    Targets targets = parseTargets(json, options);
    List<Action> actions = parseActions(json, options);
    // TODO: validate
    return new ImportSpecification("0.legacy", config, sources, targets, actions);
  }

  private static String fetchContent(String jobSpecUri) {
    try {
      return FileSystemUtils.getPathContents(jobSpecUri);
    } catch (Exception e) {
      LOG.error("Unable to fetch Neo4j job specification from URI {}: ", jobSpecUri, e);
      throw new RuntimeException(e);
    }
  }

  private static List<Source> parseSources(JSONObject json, OptionsParams options) {
    if (json.has("source")) {
      return List.of(SourceMapper.fromJson(json.getJSONObject("source"), options));
    }
    if (json.has("sources")) {
      return SourceMapper.fromJson(json.getJSONArray("sources"), options);
    }
    return List.of();
  }

  private static Targets parseTargets(JSONObject json, OptionsParams options) {
    if (!json.has("targets")) {
      // TODO: throw instead?
      return new Targets(null, null, null);
    }
    // TODO: add missing support for index_all_properties
    return TargetMapper.fromJson(json.getJSONArray("targets"), options);
  }

  private static List<Action> parseActions(JSONObject json, OptionsParams options) {
    if (!json.has("actions")) {
      return List.of();
    }
    return ActionMapper.fromJson(json.getJSONArray("actions"), options);
  }
}
