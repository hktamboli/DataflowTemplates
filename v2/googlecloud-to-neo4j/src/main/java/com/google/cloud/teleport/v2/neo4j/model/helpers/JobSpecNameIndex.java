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

import java.util.LinkedHashSet;
import java.util.Set;

class JobSpecNameIndex {

  private final Set<String> nodeTargets = new LinkedHashSet<>();
  private final Set<String> edgeTargets = new LinkedHashSet<>();
  private final Set<String> customQueryTargets = new LinkedHashSet<>();
  private final Set<String> actions = new LinkedHashSet<>();

  public void trackNode(String name) {
    nodeTargets.add(name);
  }

  public void trackEdge(String name) {
    edgeTargets.add(name);
  }

  public void trackCustomQuery(String name) {
    customQueryTargets.add(name);
  }

  public void trackAction(String name) {
    actions.add(name);
  }
}
