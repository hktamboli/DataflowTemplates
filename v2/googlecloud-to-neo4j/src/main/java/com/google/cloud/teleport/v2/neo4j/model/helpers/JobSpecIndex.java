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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

// note: this is not thread-safe at all
class JobSpecIndex {

  private final Set<String> nodeTargets = new LinkedHashSet<>();
  private final Set<String> edgeTargets = new LinkedHashSet<>();
  private final Set<String> customQueryTargets = new LinkedHashSet<>();
  private final Set<String> actions = new LinkedHashSet<>();
  private final Map<String, Supplier<List<String>>> dependencyGraph = new LinkedHashMap<>();

  private final Set<String> visitedDependencies = new HashSet<>();

  public void trackNode(String name, String executeAfter, String executeAfterName) {
    nodeTargets.add(name);
    if (executeAfter != null) {
      dependencyGraph.put(name, () -> resolveDependencies(executeAfter, executeAfterName));
    }
  }

  public void trackEdge(String name, String executeAfter, String executeAfterName) {
    edgeTargets.add(name);
    if (executeAfter != null) {
      dependencyGraph.put(name, () -> resolveDependencies(executeAfter, executeAfterName));
    }
  }

  public void trackCustomQuery(String name, String executeAfter, String executeAfterName) {
    customQueryTargets.add(name);
    if (executeAfter != null) {
      dependencyGraph.put(name, () -> resolveDependencies(executeAfter, executeAfterName));
    }
  }

  public void trackAction(String name, String executeAfter, String executeAfterName) {
    actions.add(name);
    if (executeAfter != null) {
      dependencyGraph.put(name, () -> resolveDependencies(executeAfter, executeAfterName));
    }
  }

  public List<String> getDependencies(String name) {
    visitedDependencies.clear();
    visitedDependencies.add(name);
    return readDependencies(name);
  }

  private List<String> resolveDependencies(String executeAfter, String executeAfterName) {
    var result = new LinkedHashSet<String>(0);
    switch (executeAfter.toLowerCase(Locale.ROOT)) {
      case "nodes":
        result.addAll(resolveAll(minus(nodeTargets, visitedDependencies)));
        break;
      case "edges":
        result.addAll(resolveAll(minus(edgeTargets, visitedDependencies)));
        break;
      case "custom_queries":
        result.addAll(resolveAll(minus(customQueryTargets, visitedDependencies)));
        break;
      case "node":
      case "edge":
      case "custom_query":
      case "action":
        // this is here to "break" cycles
        // the import-spec built-in validation will reject cycles later
        if (visitedDependencies.contains(executeAfterName)) {
          break;
        }
        result.add(executeAfterName);
        result.addAll(resolve(executeAfterName));
    }
    return new ArrayList<>(result);
  }

  private List<String> resolveAll(Collection<String> dependencies) {
    visitedDependencies.addAll(dependencies);
    var result = new LinkedHashSet<>(dependencies);
    dependencies.forEach(dependency -> result.addAll(resolve(dependency)));
    return new ArrayList<>(result);
  }

  private List<String> resolve(String name) {
    return resolveAll(readDependencies(name));
  }

  private List<String> readDependencies(String name) {
    return dependencyGraph
        .getOrDefault(
            name, () -> resolveAll(minus(implicitDependenciesOf(name), visitedDependencies)))
        .get();
  }

  private List<String> implicitDependenciesOf(String name) {
    if (edgeTargets.contains(name)) {
      return new ArrayList<>(nodeTargets);
    }
    if (customQueryTargets.contains(name)) {
      var result = new ArrayList<>(nodeTargets);
      result.addAll(edgeTargets);
      return result;
    }
    return List.of();
  }

  private static <T> Collection<T> minus(Collection<T> left, Collection<T> right) {
    var result = new LinkedHashSet<>(left);
    result.removeAll(right);
    return result;
  }
}
