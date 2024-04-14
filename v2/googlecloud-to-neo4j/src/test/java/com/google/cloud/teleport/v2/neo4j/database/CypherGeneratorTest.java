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
package com.google.cloud.teleport.v2.neo4j.database;

import static com.google.common.truth.Truth.assertThat;

import com.google.cloud.teleport.v2.neo4j.model.helpers.JobSpecMapper;
import com.google.cloud.teleport.v2.neo4j.model.job.OptionsParams;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeMatchMode;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipKeyConstraint;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.Targets;
import org.neo4j.importer.v1.targets.WriteMode;

public class CypherGeneratorTest {

  private static final String SPEC_PATH = "src/test/resources/testing-specs/cypher-generator-test";

  @Test
  public void specifies_keys_in_relationship_merge_pattern() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-with-keys-spec.json", new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();
    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MATCH (start:`Source` {`id`: row.`source`}) "
                + "MATCH (end:`Target` {`id`: row.`target`}) "
                + "MERGE (start)-[r:`LINKS` {`id1`: row.`rel_id_1`, `id2`: row.`rel_id_2`}]->(end) "
                + "SET r.`ts` = row.`timestamp`");
  }

  @Test
  public void specifies_only_type_in_keyless_relationship_merge_pattern() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-without-keys-spec.json",
            new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();
    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MATCH (start:`Source` {`id`: row.`source`}) "
                + "MATCH (end:`Target` {`id`: row.`target`}) "
                + "MERGE (start)-[r:`LINKS`]->(end) "
                + "SET r.`ts` = row.`timestamp`");
  }

  @Test
  public void merges_edges_as_well_as_their_start_and_end_nodes() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-merge-all.json", new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();

    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MERGE (start:`Source` {`src_id`: row.`source`}) "
                + "MERGE (end:`Target` {`tgt_id`: row.`target`}) "
                + "MERGE (start)-[r:`LINKS`]->(end) SET r.`ts` = row.`timestamp`");
  }

  @Test
  public void creates_edges_and_merges_their_start_and_end_nodes() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-create-rels-merge-nodes.json",
            new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();

    String statement = CypherGenerator.getImportStatement(importSpecification, relationshipTarget);

    assertThat(statement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MERGE (start:`Source` {`src_id`: row.`source`}) "
                + "MERGE (end:`Target` {`tgt_id`: row.`target`}) "
                + "CREATE (start)-[r:`LINKS`]->(end) SET r.`ts` = row.`timestamp`");
  }

  @Test
  public void does_not_generate_constraints_for_edge_without_schema() {
    var importSpecification =
        JobSpecMapper.parse(
            SPEC_PATH + "/single-target-relation-import-merge-all.json", new OptionsParams());
    RelationshipTarget relationshipTarget =
        importSpecification.getTargets().getRelationships().iterator().next();

    Set<String> statements = CypherGenerator.getSchemaStatements(relationshipTarget);

    assertThat(statements).isEmpty();
  }

  @Test
  public void generates_relationship_key_statement() {
    var relationship =
        new RelationshipTarget(
            true,
            "self-linking-nodes",
            "a-source",
            null,
            "SELF_LINKS_TO",
            WriteMode.MERGE,
            NodeMatchMode.MERGE,
            null,
            "node-target",
            "node-target",
            List.of(new PropertyMapping("source_field", "targetRelProperty", null)),
            new RelationshipSchema(
                null,
                List.of(
                    new RelationshipKeyConstraint("rel-key", List.of("targetRelProperty"), null)),
                null,
                null,
                null,
                null,
                null,
                null,
                null));

    Set<String> schemaStatements = CypherGenerator.getSchemaStatements(relationship);

    assertThat(schemaStatements)
        .isEqualTo(
            Set.of(
                "CREATE CONSTRAINT `rel-key` IF NOT EXISTS FOR ()-[r:`SELF_LINKS_TO`]-() REQUIRE (r.`targetRelProperty`) IS RELATIONSHIP KEY"));
  }

  @Test
  public void matches_nodes_of_relationship_to_merge() {
    var startNode =
        new NodeTarget(
            true,
            "start-node-target",
            "a-source",
            null,
            WriteMode.MERGE,
            null,
            List.of("StartNode"),
            List.of(new PropertyMapping("source_node_field", "targetNodeProperty", null)),
            new NodeSchema(
                null,
                List.of(
                    new NodeKeyConstraint(
                        "node-key", "StartNode", List.of("targetNodeProperty"), null)),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
    var endNode =
        new NodeTarget(
            true,
            "end-node-target",
            "a-source",
            null,
            WriteMode.MERGE,
            null,
            List.of("EndNode"),
            List.of(new PropertyMapping("source_node_field", "targetNodeProperty", null)),
            new NodeSchema(
                null,
                List.of(
                    new NodeKeyConstraint(
                        "node-key", "EndNode", List.of("targetNodeProperty"), null)),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
    var relationship =
        new RelationshipTarget(
            true,
            "matches-rel-nodes",
            "a-source",
            null,
            "LINKS_TO",
            WriteMode.MERGE,
            NodeMatchMode.MATCH,
            null,
            "start-node-target",
            "end-node-target",
            List.of(new PropertyMapping("source_field", "targetRelProperty", null)),
            new RelationshipSchema(
                null,
                List.of(
                    new RelationshipKeyConstraint("rel-key", List.of("targetRelProperty"), null)),
                null,
                null,
                null,
                null,
                null,
                null,
                null));
    ImportSpecification importSpecification =
        new ImportSpecification(
            "1.0",
            null,
            null,
            new Targets(List.of(startNode, endNode), List.of(relationship), null),
            null);

    String importStatement = CypherGenerator.getImportStatement(importSpecification, relationship);

    assertThat(importStatement)
        .isEqualTo(
            "UNWIND $rows AS row "
                + "MATCH (start:`StartNode` {`targetNodeProperty`: row.`source_node_field`}) "
                + "MATCH (end:`EndNode` {`targetNodeProperty`: row.`source_node_field`}) "
                + "MERGE (start)-[r:`LINKS_TO` {`targetRelProperty`: row.`source_field`}]->(end)");
  }
}
