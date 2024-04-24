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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import org.neo4j.importer.v1.actions.ActionStage;

class JobSpecIndex {

  private final Set<String> nodeTargets = new LinkedHashSet<>();
  private final Set<String> edgeTargets = new LinkedHashSet<>();
  private final Set<String> customQueryTargets = new LinkedHashSet<>();
  private final Map<String, ActionStage> actionStages = new HashMap<>();

  private final Map<String, Supplier<List<String>>> dependencyGraph =
      new LinkedHashMap<>(); // lazy map of dependencies

  private final Set<String> visitedDependencies = new HashSet<>();

  public void trackNode(String name, String executeAfter, String executeAfterName) {
    nodeTargets.add(name);
    if (executeAfter != null) {
      if (executeAfter.toLowerCase(Locale.ROOT).equals("action")) {
        var stage = actionStages.get(executeAfterName);
        if (stage == null) {
          actionStages.put(executeAfterName, ActionStage.PRE_NODES);
          return;
        }
        switch (stage) {
          case START:
          case PRE_NODES:
            return;
          case PRE_RELATIONSHIPS:
          case PRE_QUERIES:
            actionStages.put(executeAfterName, ActionStage.START);
            break;
          default:
            throw incompatibleStageException(executeAfterName, stage, ActionStage.PRE_NODES);
        }
      } else {
        dependencyGraph.put(name, () -> resolveDependencies(executeAfter, executeAfterName));
      }
    }
  }

  public void trackEdge(String name, String executeAfter, String executeAfterName) {
    edgeTargets.add(name);
    if (executeAfter != null) {
      if (executeAfter.toLowerCase(Locale.ROOT).equals("action")) {
        var stage = actionStages.get(executeAfterName);
        if (stage == null) {
          actionStages.put(executeAfterName, ActionStage.PRE_RELATIONSHIPS);
          return;
        }
        switch (stage) {
          case START:
          case PRE_RELATIONSHIPS:
            return;
          case PRE_NODES:
          case PRE_QUERIES:
            actionStages.put(executeAfterName, ActionStage.START);
            break;
          default:
            throw incompatibleStageException(
                executeAfterName, stage, ActionStage.PRE_RELATIONSHIPS);
        }
      } else {
        dependencyGraph.put(name, () -> resolveDependencies(executeAfter, executeAfterName));
      }
    }
  }

  public void trackCustomQuery(String name, String executeAfter, String executeAfterName) {
    customQueryTargets.add(name);
    if (executeAfter != null) {
      if (executeAfter.toLowerCase(Locale.ROOT).equals("action")) {
        var stage = actionStages.get(executeAfterName);
        if (stage == null) {
          actionStages.put(executeAfterName, ActionStage.PRE_QUERIES);
          return;
        }
        switch (stage) {
          case START:
          case PRE_QUERIES:
            return;
          case PRE_NODES:
          case PRE_RELATIONSHIPS:
            actionStages.put(executeAfterName, ActionStage.START);
            break;
          default:
            throw incompatibleStageException(executeAfterName, stage, ActionStage.PRE_QUERIES);
        }
      } else {
        dependencyGraph.put(name, () -> resolveDependencies(executeAfter, executeAfterName));
      }
    }
  }

  public void trackAction(String name, String executeAfter) {
    if (executeAfter == null) {
      return;
    }
    var previousStage = actionStages.get(name);
    if (previousStage == ActionStage.END) {
      return;
    }
    switch (executeAfter.toLowerCase(Locale.ROOT)) {
      case "sources":
      case "source":
        throwIfDependedUpon(name, previousStage, ActionStage.POST_SOURCES);
        actionStages.put(name, ActionStage.POST_SOURCES);
        break;
      case "nodes":
      case "node":
        throwIfDependedUpon(name, previousStage, ActionStage.POST_NODES);
        actionStages.put(name, ActionStage.POST_NODES);
        break;
      case "edges":
      case "edge":
        throwIfDependedUpon(name, previousStage, ActionStage.POST_RELATIONSHIPS);
        actionStages.put(name, ActionStage.POST_RELATIONSHIPS);
        break;
      case "custom_queries":
      case "custom_query":
        throwIfDependedUpon(name, previousStage, ActionStage.POST_QUERIES);
        actionStages.put(name, ActionStage.POST_QUERIES);
        break;
      case "action":
      case "loads":
        throwIfDependedUpon(name, previousStage, ActionStage.END);
        actionStages.put(name, ActionStage.END);
        break;
      case "start":
      case "async":
      case "preloads":
        actionStages.put(name, ActionStage.START);
        break;
    }
  }

  public List<String> getDependencies(String name) {
    // note: this is not thread-safe at all
    visitedDependencies.clear();
    visitedDependencies.add(name);
    return readDependencies(name);
  }

  public ActionStage getActionStage(String name) {
    return actionStages.getOrDefault(name, ActionStage.END);
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
        // the import-spec default validators will reject cycles later
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

  private static void throwIfDependedUpon(
      String name, ActionStage previousStage, ActionStage newStage) {
    // START is included here as it can mean several targets of different types depend on the same
    // action
    // see trackNode, trackEdge, trackCustomQuery
    if (previousStage == ActionStage.START
        || previousStage == ActionStage.PRE_NODES
        || previousStage == ActionStage.PRE_RELATIONSHIPS
        || previousStage == ActionStage.PRE_QUERIES) {
      throw incompatibleStageException(name, previousStage, newStage);
    }
  }

  private static IllegalArgumentException incompatibleStageException(
      String actionName, ActionStage previousStage, ActionStage newStage) {
    return new IllegalArgumentException(
        String.format(
            "Cannot reconcile stage for action \"%s\": it was initially %s and needs to be %s",
            actionName, previousStage, newStage));
  }
}
