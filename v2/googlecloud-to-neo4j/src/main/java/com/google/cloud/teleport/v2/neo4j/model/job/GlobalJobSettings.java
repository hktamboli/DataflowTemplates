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
package com.google.cloud.teleport.v2.neo4j.model.job;

import java.util.Collections;
import java.util.Map;

public class GlobalJobSettings {

  private final Map<String, Object> settings;

  private GlobalJobSettings(Map<String, Object> settings) {
    this.settings = settings;
  }

  public static GlobalJobSettings wrap(Map<String, Object> settings) {
    return new GlobalJobSettings(settings == null ? Collections.emptyMap() : settings);
  }

  public boolean resetDatabase() {
    return (boolean) settings.getOrDefault("reset_db", false);
  }
}
