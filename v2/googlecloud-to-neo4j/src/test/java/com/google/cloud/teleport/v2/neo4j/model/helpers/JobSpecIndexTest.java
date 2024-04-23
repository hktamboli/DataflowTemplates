/*
 * Copyright (C) 2024 Google LLC
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

import java.util.List;
import org.junit.Test;

public class JobSpecIndexTest {
  private final JobSpecIndex index = new JobSpecIndex();

  @Test
  public void resolves_node_named_explicit_dependency() {
    index.trackNode("a-node", "edge", "an-edge");
    index.trackEdge("an-edge", null, null);

    var dependencies = index.getDependencies("a-node");

    assertThat(dependencies).containsExactlyElementsIn(List.of("an-edge"));
  }

  @Test
  public void transitively_resolves_node_explicit_dependencies() {
    index.trackNode("a-node", "edge", "an-edge");
    index.trackEdge("an-edge", "custom_query", "a-query");

    var dependencies = index.getDependencies("a-node");

    assertThat(dependencies).containsExactlyElementsIn(List.of("an-edge", "a-query"));
  }

  @Test
  public void resolves_node_explicit_dependency_group() {
    index.trackCustomQuery("a-query-1", null, null);
    index.trackNode("a-node", "custom_queries", null);
    index.trackCustomQuery("a-query-2", null, null);

    var dependencies = index.getDependencies("a-node");

    assertThat(dependencies).containsExactlyElementsIn(List.of("a-query-1", "a-query-2"));
  }

  @Test
  public void transitively_resolves_node_dependency_group() {
    index.trackCustomQuery("a-query", "edges", null);
    index.trackEdge("an-edge-1", null, null);
    index.trackNode("a-node", "custom_query", "a-query");
    index.trackEdge("an-edge-2", null, null);

    var dependencies = index.getDependencies("a-node");

    assertThat(dependencies)
        .containsExactlyElementsIn(List.of("a-query", "an-edge-1", "an-edge-2"));
  }

  @Test
  public void resolves_node_peer_dependencies_excluding_itself() {
    index.trackNode("a-node-1", null, null);
    index.trackNode("a-node-2", "nodes", null);
    index.trackNode("a-node-3", null, null);

    var dependencies = index.getDependencies("a-node-2");

    assertThat(dependencies).containsExactlyElementsIn(List.of("a-node-1", "a-node-3"));
  }

  @Test
  public void transitively_resolves_node_peer_dependencies_excluding_itself() {
    index.trackCustomQuery("a-query", "edges", null);
    index.trackNode("a-node-1", null, null);
    index.trackNode("a-node-2", "custom_query", "a-query");
    index.trackNode("a-node-3", null, null);
    index.trackEdge("an-edge-1", "nodes", null);
    index.trackEdge("an-edge-2", "custom_queries", null);

    var dependencies = index.getDependencies("a-node-2");

    assertThat(dependencies)
        .containsExactlyElementsIn(
            List.of("a-query", "an-edge-1", "an-edge-2", "a-node-1", "a-node-3"));
  }

  @Test
  public void resolves_cycles() {
    index.trackNode("a-node", "edge", "an-edge");
    index.trackEdge("an-edge", "node", "a-node");

    assertThat(index.getDependencies("a-node")).containsExactlyElementsIn(List.of("an-edge"));
    assertThat(index.getDependencies("an-edge")).containsExactlyElementsIn(List.of("a-node"));
  }

  @Test
  public void resolves_implicit_node_dependencies_of_dependencyless_edge() {
    index.trackNode("a-node-1", null, null);
    index.trackNode("a-node-2", null, null);
    index.trackEdge("an-edge", null, null);
    index.trackCustomQuery("a-query", null, null);

    var dependencies = index.getDependencies("an-edge");

    assertThat(dependencies).containsExactlyElementsIn(List.of("a-node-1", "a-node-2"));
  }

  @Test
  public void transitively_resolves_implicit_node_dependencies_of_dependencyless_edge() {
    index.trackNode("a-node-1", "custom_query", "a-query-1");
    index.trackNode("a-node-2", null, null);
    index.trackEdge("an-edge", null, null);
    index.trackCustomQuery("a-query-1", "custom_queries", null);
    index.trackCustomQuery("a-query-2", null, null);

    var dependencies = index.getDependencies("an-edge");

    assertThat(dependencies)
        .containsExactlyElementsIn(List.of("a-node-1", "a-node-2", "a-query-1", "a-query-2"));
  }

  @Test
  public void resolves_implicit_node_and_edge_dependencies_of_dependencyless_query() {
    index.trackNode("a-node-1", null, null);
    index.trackNode("a-node-2", null, null);
    index.trackEdge("an-edge", null, null);
    index.trackCustomQuery("a-query", null, null);

    var dependencies = index.getDependencies("an-edge");

    assertThat(dependencies).containsExactlyElementsIn(List.of("a-node-1", "a-node-2"));
  }

  @Test
  public void transitively_resolves_implicit_node_and_edge_dependencies_of_dependencyless_query() {
    index.trackNode("a-node-1", "custom_queries", null);
    index.trackNode("a-node-2", null, null);
    index.trackEdge("an-edge", "custom_query", "a-query-3");
    index.trackCustomQuery("a-query-1", null, null);
    index.trackCustomQuery("a-query-2", null, null);
    index.trackCustomQuery("a-query-3", null, null);

    var dependencies = index.getDependencies("a-query-1");

    assertThat(dependencies)
        .containsExactlyElementsIn(
            List.of("a-node-1", "a-node-2", "an-edge", "a-query-2", "a-query-3"));
  }
}
