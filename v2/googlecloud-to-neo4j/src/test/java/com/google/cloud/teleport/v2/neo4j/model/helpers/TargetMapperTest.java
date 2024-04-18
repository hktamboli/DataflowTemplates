/*
 * Copyright (C) 2023 Google LLC
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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Map.entry;

import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.neo4j.importer.v1.targets.Aggregation;
import org.neo4j.importer.v1.targets.NodeExistenceConstraint;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeMatchMode;
import org.neo4j.importer.v1.targets.NodeRangeIndex;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.NodeUniqueConstraint;
import org.neo4j.importer.v1.targets.Order;
import org.neo4j.importer.v1.targets.OrderBy;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.PropertyType;
import org.neo4j.importer.v1.targets.SourceTransformations;
import org.neo4j.importer.v1.targets.Targets;
import org.neo4j.importer.v1.targets.WriteMode;

@SuppressWarnings("deprecation")
public class TargetMapperTest {

  @Test
  public void parses_minimal_node_target() {
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name", "",
                    "mode", "append",
                    "mappings", Map.of("label", "\"Placeholder\""))));

    var targets =
        TargetMapper.parse(new JSONArray(List.of(nodeTarget)), new OptionsParams(), false);

    assertThat(targets.getNodes())
        .isEqualTo(
            List.of(
                new NodeTarget(
                    true,
                    "node/0",
                    "",
                    null,
                    WriteMode.CREATE,
                    null,
                    List.of("Placeholder"),
                    List.of(),
                    null)));
    assertThat(targets.getRelationships()).isEmpty();
    assertThat(targets.getCustomQueries()).isEmpty();
  }

  @Test // TODO: dependencies
  public void parses_node_target() {
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "active",
                    false,
                    "name",
                    "a-target",
                    "source",
                    "a-source",
                    "mode",
                    "merge",
                    "transform",
                    Map.of(
                        "group",
                        true,
                        "aggregations",
                        List.of(
                            Map.of("field", "field01", "expr", "42"),
                            Map.of("field", "field02", "expr", "24")),
                        "where",
                        "1+2=3",
                        "order_by",
                        "field01 ASC, field02 DESC",
                        "limit",
                        100),
                    "mappings",
                    Map.of(
                        "labels", List.of("\"Placeholder1\"", "\"Placeholder2\""),
                        "properties",
                            Map.<String, Object>ofEntries(
                                entry("key", List.of(Map.of("field01", "prop01"))),
                                entry("keys", List.of(Map.of("field02", "prop02"))),
                                entry("unique", List.of(Map.of("field03", "prop03"))),
                                entry("mandatory", List.of(Map.of("field04", "prop04"))),
                                entry("indexed", List.of(Map.of("field05", "prop05"))),
                                entry("dates", List.of(Map.of("field06", "prop06"))),
                                entry("doubles", List.of(Map.of("field07", "prop07"))),
                                entry("floats", List.of(Map.of("field08", "prop08"))),
                                entry("longs", List.of(Map.of("field09", "prop09"))),
                                entry("integers", List.of(Map.of("field10", "prop10"))),
                                entry("strings", List.of(Map.of("field11", "prop11"))),
                                entry("points", List.of(Map.of("field12", "prop12"))),
                                entry("booleans", List.of(Map.of("field13", "prop13"))),
                                entry(
                                    "bytearrays",
                                    List.of(Map.of("field14", "prop14"), "prop15")))))));

    var targets =
        TargetMapper.parse(new JSONArray(List.of(nodeTarget)), new OptionsParams(), false);

    var expectedTarget =
        new NodeTarget(
            false,
            "node/a-target",
            "a-source",
            null,
            WriteMode.MERGE,
            new SourceTransformations(
                true,
                List.of(new Aggregation("42", "field01"), new Aggregation("24", "field02")),
                "1+2=3",
                List.of(new OrderBy("field01", Order.ASC), new OrderBy("field02", Order.DESC)),
                100),
            List.of("Placeholder1", "Placeholder2"),
            List.of(
                new PropertyMapping("field01", "prop01", null),
                new PropertyMapping("field02", "prop02", null),
                new PropertyMapping("field03", "prop03", null),
                new PropertyMapping("field04", "prop04", null),
                new PropertyMapping("field05", "prop05", null),
                new PropertyMapping("field06", "prop06", PropertyType.ZONED_DATETIME),
                new PropertyMapping("field07", "prop07", PropertyType.FLOAT),
                new PropertyMapping("field08", "prop08", PropertyType.FLOAT),
                new PropertyMapping("field09", "prop09", PropertyType.INTEGER),
                new PropertyMapping("field10", "prop10", PropertyType.INTEGER),
                new PropertyMapping("field11", "prop11", PropertyType.STRING),
                new PropertyMapping("field12", "prop12", PropertyType.POINT),
                new PropertyMapping("field13", "prop13", PropertyType.BOOLEAN),
                new PropertyMapping("field14", "prop14", PropertyType.BYTE_ARRAY),
                new PropertyMapping("prop15", "prop15", PropertyType.BYTE_ARRAY)),
            new NodeSchema(
                null,
                List.of(
                    new NodeKeyConstraint(
                        "node/a-target-Placeholder1-node-single-key-for-prop01",
                        "Placeholder1",
                        List.of("prop01"),
                        null),
                    new NodeKeyConstraint(
                        "node/a-target-Placeholder2-node-single-key-for-prop01",
                        "Placeholder2",
                        List.of("prop01"),
                        null),
                    new NodeKeyConstraint(
                        "node/a-target-Placeholder1-node-key-for-prop02",
                        "Placeholder1",
                        List.of("prop02"),
                        null),
                    new NodeKeyConstraint(
                        "node/a-target-Placeholder2-node-key-for-prop02",
                        "Placeholder2",
                        List.of("prop02"),
                        null)),
                List.of(
                    new NodeUniqueConstraint(
                        "node/a-target-Placeholder1-node-unique-for-prop03",
                        "Placeholder1",
                        List.of("prop03"),
                        null),
                    new NodeUniqueConstraint(
                        "node/a-target-Placeholder2-node-unique-for-prop03",
                        "Placeholder2",
                        List.of("prop03"),
                        null)),
                List.of(
                    new NodeExistenceConstraint(
                        "node/a-target-Placeholder1-node-not-null-for-prop04",
                        "Placeholder1",
                        "prop04"),
                    new NodeExistenceConstraint(
                        "node/a-target-Placeholder2-node-not-null-for-prop04",
                        "Placeholder2",
                        "prop04")),
                List.of(
                    new NodeRangeIndex(
                        "node/a-target-Placeholder1-node-range-index-for-prop05",
                        "Placeholder1",
                        List.of("prop05")),
                    new NodeRangeIndex(
                        "node/a-target-Placeholder2-node-range-index-for-prop05",
                        "Placeholder2",
                        List.of("prop05"))),
                null,
                null,
                null,
                null));
    assertThat(targets.getNodes()).isEqualTo(List.of(expectedTarget));
    assertThat(targets.getRelationships()).isEmpty();
    assertThat(targets.getCustomQueries()).isEmpty();
  }

  @Test
  public void parses_node_target_indexing_all_properties() {
    var nodeTarget =
        new JSONObject(
            Map.of(
                "node",
                Map.of(
                    "name", "a-target",
                    "source", "a-source",
                    "mode", "merge",
                    "mappings",
                        Map.of(
                            "labels", List.of("\"Placeholder\""),
                            "properties",
                                Map.<String, Object>ofEntries(
                                    entry("key", List.of(Map.of("field01", "prop01"))),
                                    entry("keys", List.of(Map.of("field02", "prop02"))),
                                    entry("unique", List.of(Map.of("field03", "prop03"))),
                                    entry("mandatory", List.of(Map.of("field04", "prop04"))),
                                    entry("indexed", List.of(Map.of("field05", "prop05"))),
                                    entry("dates", List.of(Map.of("field06", "prop06"))),
                                    entry("doubles", List.of(Map.of("field07", "prop07"))),
                                    entry("floats", List.of(Map.of("field08", "prop08"))),
                                    entry("longs", List.of(Map.of("field09", "prop09"))),
                                    entry("integers", List.of(Map.of("field10", "prop10"))),
                                    entry("strings", List.of(Map.of("field11", "prop11"))),
                                    entry("points", List.of(Map.of("field12", "prop12"))),
                                    entry("booleans", List.of(Map.of("field13", "prop13"))),
                                    entry(
                                        "bytearrays",
                                        List.of(Map.of("field14", "prop14"), "prop15")))))));

    var targets = TargetMapper.parse(new JSONArray(List.of(nodeTarget)), new OptionsParams(), true);

    var expectedSchema =
        new NodeSchema(
            null,
            List.of(
                new NodeKeyConstraint(
                    "node/a-target-Placeholder-node-single-key-for-prop01",
                    "Placeholder",
                    List.of("prop01"),
                    null),
                new NodeKeyConstraint(
                    "node/a-target-Placeholder-node-key-for-prop02",
                    "Placeholder",
                    List.of("prop02"),
                    null)),
            List.of(
                new NodeUniqueConstraint(
                    "node/a-target-Placeholder-node-unique-for-prop03",
                    "Placeholder",
                    List.of("prop03"),
                    null)),
            List.of(
                new NodeExistenceConstraint(
                    "node/a-target-Placeholder-node-not-null-for-prop04", "Placeholder", "prop04")),
            List.of(
                new NodeRangeIndex(
                    "node/a-target-Placeholder-node-range-index-for-prop05",
                    "Placeholder",
                    List.of("prop05")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop04", "Placeholder", List.of("prop04")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop06", "Placeholder", List.of("prop06")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop07", "Placeholder", List.of("prop07")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop08", "Placeholder", List.of("prop08")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop09", "Placeholder", List.of("prop09")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop10", "Placeholder", List.of("prop10")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop11", "Placeholder", List.of("prop11")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop12", "Placeholder", List.of("prop12")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop13", "Placeholder", List.of("prop13")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop14", "Placeholder", List.of("prop14")),
                new NodeRangeIndex(
                    "node/default-index-for-Placeholder-prop15", "Placeholder", List.of("prop15"))),
            null,
            null,
            null,
            null);
    var target = targets.getNodes().iterator().next();
    assertThat(target.getSchema()).isEqualTo(expectedSchema);
  }

  @Test
  public void parses_custom_query_target() {
    JSONObject jsonTarget = jsonTargetOfType("custom_query");
    JSONObject customObject = jsonTarget.getJSONObject("custom_query");
    customObject.put("query", "UNWIND $rows AS row CREATE (:Node {prop: row.prop})");

    Targets targets =
        TargetMapper.parse(new JSONArray(List.of(jsonTarget)), new OptionsParams(), false);

    assertThat(targets.getNodes()).isEmpty();
    assertThat(targets.getRelationships()).isEmpty();
    assertThat(targets.getCustomQueries()).hasSize(1);
    var target = targets.getCustomQueries().get(0);
    assertThat(target.getQuery()).isEqualTo("UNWIND $rows AS row CREATE (:Node {prop: row.prop})");
  }

  @Test
  public void sets_specified_match_mode_for_edge_targets() {
    JSONObject target = jsonTargetOfType("edge");
    JSONObject edge = target.getJSONObject("edge");
    edge.put("mode", "merge");
    edge.put("edge_nodes_match_mode", "merge");
    edge.put(
        "mappings",
        Map.of(
            "type", "TYPE",
            "source", Map.of("label", "Placeholder1", "key", "key1"),
            "target", Map.of("label", "Placeholder2", "key", "key2")));

    Targets targets =
        TargetMapper.parse(new JSONArray(List.of(target)), new OptionsParams(), false);

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MERGE);
  }

  @Test
  public void sets_specified_match_mode_for_edge_targets_in_append_mode() {
    JSONObject target = jsonTargetOfType("edge");
    JSONObject edge = target.getJSONObject("edge");
    edge.put("mode", "append");
    edge.put("edge_nodes_match_mode", "merge");
    edge.put(
        "mappings",
        Map.of(
            "type", "TYPE",
            "source", Map.of("label", "Placeholder1", "key", "key1"),
            "target", Map.of("label", "Placeholder2", "key", "key2")));

    Targets targets =
        TargetMapper.parse(new JSONArray(List.of(target)), new OptionsParams(), false);

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MERGE);
  }

  @Test
  public void sets_default_node_match_mode_for_edge_targets_to_merge() {
    JSONObject target = jsonTargetOfType("edge");
    JSONObject edge = target.getJSONObject("edge");
    edge.put("mode", "merge");
    edge.put(
        "mappings",
        Map.of(
            "type", "TYPE",
            "source", Map.of("label", "Placeholder1", "key", "key1"),
            "target", Map.of("label", "Placeholder2", "key", "key2")));

    Targets targets =
        TargetMapper.parse(new JSONArray(List.of(target)), new OptionsParams(), false);

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MATCH);
  }

  @Test
  public void sets_default_node_match_mode_for_edge_targets_to_create() {
    JSONObject target = jsonTargetOfType("edge");
    JSONObject edge = target.getJSONObject("edge");
    edge.put("mode", "append");
    edge.put(
        "mappings",
        Map.of(
            "type", "TYPE",
            "source", Map.of("label", "Placeholder1", "key", "key1"),
            "target", Map.of("label", "Placeholder2", "key", "key2")));

    Targets targets =
        TargetMapper.parse(new JSONArray(List.of(target)), new OptionsParams(), false);

    assertThat(targets.getRelationships()).hasSize(1);
    assertThat(targets.getRelationships().get(0).getNodeMatchMode()).isEqualTo(NodeMatchMode.MERGE);
  }

  @Test
  public void parses_simple_order_by() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", null)));
  }

  @Test
  public void parses_simple_list_of_order_by() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col, col2"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", null), new OrderBy("col2", null)));
  }

  @Test
  public void parses_order_by_with_direction() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col DESC"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses).isEqualTo(List.of(new OrderBy("col", Order.DESC)));
  }

  @Test
  public void parses_order_by_list_with_direction() {
    JSONObject orderBy = new JSONObject(Map.of("order_by", "col DESC, col2 ASC"));

    List<OrderBy> clauses = TargetMapper.parseOrderBy(orderBy);

    assertThat(clauses)
        .isEqualTo(List.of(new OrderBy("col", Order.DESC), new OrderBy("col2", Order.ASC)));
  }

  private static JSONObject jsonTargetOfType(String type) {
    var target = new JSONObject();
    target.put("name", UUID.randomUUID().toString());
    target.put("mappings", new JSONObject());
    var result = new JSONObject();
    result.put(type, target);
    return result;
  }
}
