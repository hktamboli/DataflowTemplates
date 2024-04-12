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

import com.google.cloud.teleport.v2.neo4j.model.enums.ArtifactType;
import com.google.cloud.teleport.v2.neo4j.model.enums.TargetType;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
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
class TargetMapper {
  private static final Pattern ORDER_PATTERN = Pattern.compile("\\basc|desc\\b");

  public static void index(JSONArray json, JobSpecNameIndex index) {
    // contrary to parse, embedded node definitions do not matter here as they cannot be depended
    // upon
    for (int i = 0; i < json.length(); i++) {
      var target = json.getJSONObject(i);
      if (target.has("node")) {
        var node = target.getJSONObject("node");
        index.trackNode(normalizeName(i, node.getString("name"), ArtifactType.node));
        continue;
      }
      if (target.has("edge")) {
        var edge = target.getJSONObject("edge");
        index.trackEdge(normalizeName(i, edge.getString("name"), ArtifactType.edge));
        continue;
      }
      if (target.has("custom_query")) {
        var query = target.getJSONObject("custom_query");
        index.trackCustomQuery(
            normalizeName(i, query.getString("name"), ArtifactType.custom_query));
      }
    }
  }

  public static Targets parse(JSONArray json, OptionsParams options) {
    // TODO: add missing support for index_all_properties
    List<NodeTarget> nodes = new ArrayList<>();
    List<RelationshipTarget> relationshipTargets = new ArrayList<>();
    List<CustomQueryTarget> queryTargets = new ArrayList<>();

    // first pass: go through all nodes
    for (int i = 0; i < json.length(); i++) {
      var target = json.getJSONObject(i);
      if (!target.has("node")) {
        continue;
      }
      nodes.add(parseNode(i, target.getJSONObject("node")));
    }

    // second pass: go through edge targets' source/target nodes
    for (int i = 0; i < json.length(); i++) {
      var target = json.getJSONObject(i);
      if (!target.has("edge")) {
        continue;
      }
      var edge = target.getJSONObject("edge");
      var matchMode = parseNodeMatchMode(edge, parseWriteMode(edge.getString("mode")));
      if (findNodeTarget(edge, "source", nodes).isEmpty()) {
        nodes.add(newEdgeNodeTarget(edge, "source", matchMode));
      }
      if (findNodeTarget(edge, "target", nodes).isEmpty()) {
        nodes.add(newEdgeNodeTarget(edge, "target", matchMode));
      }
    }

    // third pass: go through edge & custom query targets
    for (int i = 0; i < json.length(); i++) {
      var target = json.getJSONObject(i);
      if (target.has("edge")) {
        relationshipTargets.add(parseEdge(i, target.getJSONObject("edge"), nodes));
        continue;
      }
      if (target.has("custom_query")) {
        queryTargets.add(parseCustomQuery(i, target.getJSONObject("custom_query"), options));
      }
    }
    return new Targets(nodes, relationshipTargets, queryTargets);
  }

  private static NodeTarget parseNode(int index, JSONObject node) {
    JSONObject mappings = node.getJSONObject("mappings");
    List<String> labels = parseLabels(mappings);
    String targetName = normalizeName(index, node.getString("name"), ArtifactType.node);
    return new NodeTarget(
        getBooleanOrDefault(node, "active", true),
        targetName,
        getStringOrDefault(node, "source", DEFAULT_SOURCE_NAME),
        null, // TODO: process dependencies
        parseWriteMode(node.getString("mode")),
        parseSourceTransformations(node),
        labels,
        parseMappings(mappings),
        parseNodeSchema(targetName, labels, mappings));
  }

  private static RelationshipTarget parseEdge(int index, JSONObject edge, List<NodeTarget> nodes) {
    WriteMode writeMode = parseWriteMode(edge.getString("mode"));
    NodeMatchMode nodeMatchMode = parseNodeMatchMode(edge, writeMode);
    JSONObject mappings = edge.getJSONObject("mappings");
    String targetName = normalizeName(index, edge.getString("name"), ArtifactType.edge);
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
        findNodeTarget(edge, "source", nodes).get(),
        findNodeTarget(edge, "target", nodes).get(),
        parseMappings(mappings),
        parseEdgeSchema(targetName, relationshipType, mappings));
  }

  private static CustomQueryTarget parseCustomQuery(
      int index, JSONObject query, OptionsParams options) {
    String cypher =
        ModelUtils.replaceVariableTokens(query.getString("query"), options.getTokenMap());
    String targetName = normalizeName(index, query.getString("name"), ArtifactType.custom_query);
    return new CustomQueryTarget(
        getBooleanOrDefault(query, "active", true),
        targetName,
        getStringOrDefault(query, "source", DEFAULT_SOURCE_NAME),
        null, // TODO: process dependencies
        cypher);
  }

  private static String normalizeName(int index, String name, ArtifactType artifactType) {
    if (name.isEmpty()) {
      return String.format("%s/%d", artifactType, index);
    }
    // make sure name is globally unique
    return String.format("%s/%s", artifactType, name);
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

  private static NodeTarget newEdgeNodeTarget(
      JSONObject edge, String key, NodeMatchMode matchMode) {
    return parseEdgeNode(edge, key, asNodeWriteMode(matchMode));
  }

  private static WriteMode parseWriteMode(String mode) {
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

  private static NodeMatchMode parseNodeMatchMode(JSONObject edge, WriteMode writeMode) {
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
