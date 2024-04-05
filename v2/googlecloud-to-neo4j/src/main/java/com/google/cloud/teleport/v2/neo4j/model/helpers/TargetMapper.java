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

import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getBooleanOrDefault;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getIntegerOrNull;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.JsonObjects.getStringOrDefault;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingMapper.findNodeTarget;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingMapper.parseEdgeNode;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingMapper.parseEdgeSchema;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingMapper.parseLabels;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingMapper.parseMappings;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingMapper.parseNodeSchema;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.MappingMapper.parseType;
import static com.google.cloud.teleport.v2.neo4j.model.helpers.SourceMapper.DEFAULT_SOURCE_NAME;

import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.importer.v1.targets.Aggregation;
import org.neo4j.importer.v1.targets.CustomQueryTarget;
import org.neo4j.importer.v1.targets.NodeMatchMode;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.Order;
import org.neo4j.importer.v1.targets.OrderBy;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.SourceTransformations;
import org.neo4j.importer.v1.targets.Targets;
import org.neo4j.importer.v1.targets.WriteMode;

/**
 * Helper class for parsing legacy json into an {@link org.neo4j.importer.v1.ImportSpecification}'s
 * {@link Targets}.
 *
 * @deprecated use the current JSON format instead
 */
@Deprecated
public class TargetMapper {
  private static final Pattern ORDER_PATTERN = Pattern.compile("\\basc|desc\\b");

  public static Targets fromJson(JSONArray json) {
    // TODO: inject default name if (active) target name is empty (see InputRefactoring)
    List<NodeTarget> nodes = new ArrayList<>();
    List<RelationshipTarget> relationshipTargets = new ArrayList<>();
    List<CustomQueryTarget> queryTargets = new ArrayList<>();
    for (int i = 0; i < json.length(); i++) {
      var target = json.getJSONObject(i);
      if (target.has("node")) {
        nodes.add(parseNode(target.getJSONObject("node")));
        continue;
      }
      if (target.has("edge")) {
        relationshipTargets.add(parseEdge(target.getJSONObject("edge"), nodes));
        continue;
      }
      if (target.has("custom_query")) {
        queryTargets.add(parseCustomQuery(target.getJSONObject("custom_query")));
        continue;
      }
      throw invalidTargetException(target);
    }
    return new Targets(nodes, relationshipTargets, queryTargets);
  }

  private static NodeTarget parseNode(JSONObject node) {
    JSONObject mappings = node.getJSONObject("mappings");
    List<String> labels = parseLabels(mappings);
    String targetName = node.getString("name");
    return new NodeTarget(
        getBooleanOrDefault(node, "active", true),
        targetName,
        getStringOrDefault(node, "source", DEFAULT_SOURCE_NAME),
        null, // TODO: process dependencies
        asWriteMode(node.getString("mode")),
        parseSourceTransformations(node),
        labels,
        parseMappings(mappings),
        parseNodeSchema(targetName, labels, mappings));
  }

  private static RelationshipTarget parseEdge(JSONObject edge, List<NodeTarget> nodes) {
    WriteMode writeMode = asWriteMode(edge.getString("mode"));
    NodeMatchMode nodeMatchMode = asNodeMatchMode(edge, writeMode);
    String startNodeReference =
        findNodeTargetOrCreate(edge, "source", nodeMatchMode, nodes).getName();
    String endNodeReference =
        findNodeTargetOrCreate(edge, "target", nodeMatchMode, nodes).getName();
    JSONObject mappings = edge.getJSONObject("mappings");
    String targetName = edge.getString("name");
    String relationshipType = parseType(mappings);
    return new RelationshipTarget(
        getBooleanOrDefault(edge, "active", true),
        targetName,
        getStringOrDefault(edge, "source", DEFAULT_SOURCE_NAME),
        null, // TODO: process dependencies
        relationshipType,
        writeMode,
        nodeMatchMode,
        parseSourceTransformations(edge),
        startNodeReference,
        endNodeReference,
        parseMappings(mappings),
        parseEdgeSchema(targetName, relationshipType, mappings));
  }

  private static CustomQueryTarget parseCustomQuery(JSONObject query) {
    return new CustomQueryTarget(
        getBooleanOrDefault(query, "active", true),
        query.getString("name"),
        getStringOrDefault(query, "source", DEFAULT_SOURCE_NAME),
        null, // TODO: process dependencies
        query.getString("query"));
  }

  private static SourceTransformations parseSourceTransformations(JSONObject json) {
    if (!json.has("transform")) {
      return null;
    }
    var transform = json.getJSONObject("transform");
    return new SourceTransformations(
        getBooleanOrDefault(transform, "group", false),
        parseAggregations(transform),
        getStringOrDefault(transform, "where", ""),
        parseOrderBy(transform),
        getIntegerOrNull(transform, "limit"));
  }

  private static List<Aggregation> parseAggregations(JSONObject json) {
    if (!json.has("aggregations")) {
      return null;
    }
    JSONArray aggregations = json.getJSONArray("aggregations");
    List<Aggregation> results = new ArrayList<>(aggregations.length());
    for (int i = 0; i < aggregations.length(); i++) {
      var aggregation = aggregations.getJSONObject(i);
      results.add(new Aggregation(aggregation.getString("expr"), aggregation.getString("field")));
    }
    return results;
  }

  // visible for testing
  static List<OrderBy> parseOrderBy(JSONObject json) {
    if (!json.has("order_by")) {
      return null;
    }
    var orderBy = json.getString("order_by");
    String[] rawClauses = StringUtils.stripAll(orderBy.split(","));
    return Arrays.stream(rawClauses)
        .map(
            clause -> {
              String clauseLowerCase = clause.toLowerCase(Locale.ROOT);
              int lastPosition = findLastIndexOfMatch(ORDER_PATTERN, clauseLowerCase);
              if (lastPosition == -1) {
                return new OrderBy(clause, null);
              }
              String expression = clause.substring(0, lastPosition).trim();
              Order order = clauseLowerCase.charAt(lastPosition) == 'a' ? Order.ASC : Order.DESC;
              return new OrderBy(expression, order);
            })
        .collect(Collectors.toList());
  }

  private static IllegalArgumentException invalidTargetException(JSONObject target) {
    String error =
        String.format(
            "Expected target JSON to have one of: \"%s\" as top-level field, but only found fields: \"%s\"",
            Arrays.stream(TargetType.values())
                .map(TargetType::name)
                .collect(Collectors.joining("\", \"")),
            String.join("\", \"", target.keySet()));
    return new IllegalArgumentException(error);
  }

  private static NodeTarget findNodeTargetOrCreate(
      JSONObject edge, String key, NodeMatchMode matchMode, List<NodeTarget> nodes) {
    return findNodeTarget(edge, key, nodes)
        .orElseGet(
            () -> {
              WriteMode nodeWriteMode = asNodeWriteMode(matchMode);
              NodeTarget sourceNode = parseEdgeNode(edge, key, nodeWriteMode);
              nodes.add(sourceNode);
              return sourceNode;
            });
  }

  private static WriteMode asWriteMode(String mode) {
    switch (mode) {
      case "append":
        return WriteMode.CREATE;
      case "merge":
        return WriteMode.MERGE;
      default:
        throw new IllegalArgumentException(
            "Unsupported node \"%s\": expected one of \"append\", \"merge\"");
    }
  }

  private static WriteMode asNodeWriteMode(NodeMatchMode matchMode) {
    switch (matchMode) {
      case MATCH:
        return WriteMode.CREATE;
      case MERGE:
        return WriteMode.MERGE;
    }
    throw new IllegalArgumentException(
        String.format(
            "expected node match mode to be either match or merge, but got: %s", matchMode));
  }

  private static NodeMatchMode asNodeMatchMode(JSONObject edge, WriteMode writeMode) {
    if (!edge.has("edge_nodes_match_mode")) {
      return defaultNodeMatchModeFor(writeMode);
    }
    return NodeMatchMode.valueOf(edge.getString("edge_nodes_match_mode").toUpperCase(Locale.ROOT));
  }

  private static NodeMatchMode defaultNodeMatchModeFor(WriteMode writeMode) {
    switch (writeMode) {
      case CREATE:
        return NodeMatchMode.MERGE;
      case MERGE:
        return NodeMatchMode.MATCH;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Cannot determine default node match mode: unsupported write mode %s", writeMode));
    }
  }

  private static int findLastIndexOfMatch(Pattern pattern, String input) {
    int lastPosition = -1;
    var matcher = pattern.matcher(input);
    while (matcher.find()) {
      lastPosition = matcher.start();
    }
    return lastPosition;
  }
}
