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
import com.google.cloud.teleport.v2.neo4j.utils.ModelUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.importer.v1.actions.Action;
import org.neo4j.importer.v1.actions.ActionStage;
import org.neo4j.importer.v1.actions.BigQueryAction;
import org.neo4j.importer.v1.actions.CypherAction;
import org.neo4j.importer.v1.actions.CypherExecutionMode;
import org.neo4j.importer.v1.actions.HttpAction;
import org.neo4j.importer.v1.actions.HttpMethod;

/**
 * Helper class for parsing legacy json into {@link Action}.
 *
 * @deprecated use the current JSON format instead
 */
@Deprecated
class ActionMapper {

  public static void index(JSONArray json, JobSpecNameIndex index) {
    for (int i = 0; i < json.length(); i++) {
      JSONObject action = json.getJSONObject(i);
      index.trackAction(action.getString("name"));
    }
  }

  public static List<Action> parse(JSONArray json, OptionsParams options) {
    List<Action> actions = new ArrayList<>(json.length());
    for (int i = 0; i < json.length(); i++) {
      actions.add(parse(json.getJSONObject(i), options));
    }
    return actions;
  }

  private static Action parse(JSONObject json, OptionsParams options) {
    String type = json.getString("type").toLowerCase(Locale.ROOT);
    boolean active = JsonObjects.getBooleanOrDefault(json, "active", true);
    String name = json.getString("name");
    var actionOptions = json.getJSONArray("options");
    if (actionOptions.length() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "Expected a single option for Cypher query, got %d", actionOptions.length()));
    }
    var option = actionOptions.getJSONObject(0);
    switch (type) {
      case "cypher":
        return new CypherAction(
            active,
            name,
            mapStage(json.opt("execute_after"), json.opt("execute_after_name")),
            ModelUtils.replaceVariableTokens(option.getString("cypher"), options.getTokenMap()),
            CypherExecutionMode.AUTOCOMMIT);
      case "bigquery":
        return new BigQueryAction(
            active,
            name,
            mapStage(json.opt("execute_after"), json.opt("execute_after_name")),
            ModelUtils.replaceVariableTokens(option.getString("sql"), options.getTokenMap()));
      case "http_get":
        return new HttpAction(
            active,
            name,
            mapStage(json.opt("execute_after"), json.opt("execute_after_name")),
            ModelUtils.replaceVariableTokens(option.getString("url"), options.getTokenMap()),
            HttpMethod.GET,
            processValues(json.getJSONObject("headers").toMap(), options));
      case "http_post":
        return new HttpAction(
            active,
            name,
            mapStage(json.opt("execute_after"), json.opt("execute_after_name")),
            ModelUtils.replaceVariableTokens(option.getString("url"), options.getTokenMap()),
            HttpMethod.POST,
            processValues(json.getJSONObject("headers").toMap(), options));
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported action type %s, expected one of: cypher, bigquery, http_get, http_post",
                type));
    }
  }

  private static ActionStage mapStage(Object executeAfter, Object executeAfterName) {
    if (executeAfter == null) {
      return ActionStage.END;
    }
    return ActionStage.START;
  }

  private static Map<String, String> processValues(Map<String, Object> map, OptionsParams options) {
    Map<String, String> result = new HashMap<>(map.size());
    for (String key : map.keySet()) {
      String value = (String) map.get("key");
      result.put(key, ModelUtils.replaceVariableTokens(value, options.getTokenMap()));
    }
    return result;
  }
}
