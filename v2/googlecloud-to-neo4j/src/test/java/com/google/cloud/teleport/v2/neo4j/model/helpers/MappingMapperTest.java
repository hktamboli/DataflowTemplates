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

import org.json.JSONObject;
import org.junit.Test;
import org.neo4j.importer.v1.targets.NodeExistenceConstraint;
import org.neo4j.importer.v1.targets.NodeKeyConstraint;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipExistenceConstraint;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.WriteMode;

import java.util.List;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static org.neo4j.importer.v1.targets.PropertyType.BOOLEAN;

public class MappingMapperTest {

    @Test
    public void parsesMultipleKeyMappingsFromObjectForEdgeSourceNode() {
        var sourceNode = Map.of("source", Map.of("label", "Placeholder", "key", Map.of("key1", "value1", "key2", "value2")));
        var edge =
                new JSONObject(
                        Map.of(
                                "name", "an-edge",
                                "source", "a-source",
                                "mappings", sourceNode));

        NodeTarget result = MappingMapper.parseEdgeNode(edge, "source", WriteMode.CREATE);

        assertThat(result)
                .isEqualTo(
                        new NodeTarget(
                                true,
                                "an-edge-source",
                                "a-source",
                                null,
                                WriteMode.CREATE,
                                null,
                                List.of("Placeholder"),
                                List.of(
                                        new PropertyMapping("key1", "value1", null),
                                        new PropertyMapping("key2", "value2", null)
                                ),
                                nodeKeys(List.of(new NodeKeyConstraint(
                                        "an-edge-source-node-single-key-for-value1-value2",
                                        "Placeholder",
                                        List.of("value1", "value2"),
                                        null
                                )))
                        ));
    }

    @Test
    public void parsesMultipleKeyMappingsFromObjectArrayForEdgeSourceNode() {
        var sourceNode = Map.of("source", Map.of(
                "label",
                "Placeholder",
                "keys",
                List.of(
                        Map.of("key1", "value1", "key2", "value2"),
                        Map.of("key3", "value3", "key4", "value4"))));
        var edge =
                new JSONObject(
                        Map.of(
                                "name", "an-edge",
                                "source", "a-source",
                                "mappings", sourceNode));

        NodeTarget result = MappingMapper.parseEdgeNode(edge, "source", WriteMode.MERGE);

        assertThat(result)
                .isEqualTo(
                        new NodeTarget(
                                true,
                                "an-edge-source",
                                "a-source",
                                null,
                                WriteMode.MERGE,
                                null,
                                List.of("Placeholder"),
                                List.of(
                                        new PropertyMapping("key1", "value1", null),
                                        new PropertyMapping("key2", "value2", null),
                                        new PropertyMapping("key3", "value3", null),
                                        new PropertyMapping("key4", "value4", null)
                                ),
                                nodeKeys(List.of(new NodeKeyConstraint(
                                        "an-edge-source-node-key-for-value1-value2",
                                        "Placeholder",
                                        List.of("value1", "value2"),
                                        null
                                ), new NodeKeyConstraint(
                                        "an-edge-source-node-key-for-value3-value4",
                                        "Placeholder",
                                        List.of("value3", "value4"),
                                        null
                                )))
                        ));
    }

    @Test
    public void parsesMultipleKeyMappingsFromStringArrayForEdgeSourceNode() {
        var sourceNode =
                new JSONObject(
                        Map.of("source", Map.of("label", "Placeholder", "key", List.of("value1", "value2"))));
        var edge =
                new JSONObject(
                        Map.of(
                                "name", "an-edge",
                                "source", "a-source",
                                "mappings", sourceNode));

        NodeTarget result = MappingMapper.parseEdgeNode(edge, "source", WriteMode.CREATE);

        assertThat(result)
                .isEqualTo(
                        new NodeTarget(
                                true,
                                "an-edge-source",
                                "a-source",
                                null,
                                WriteMode.CREATE,
                                null,
                                List.of("Placeholder"),
                                List.of(
                                        new PropertyMapping("value1", "value1", null),
                                        new PropertyMapping("value1", "value2", null)
                                ),
                                nodeKeys(List.of(new NodeKeyConstraint(
                                        "an-edge-source-node-single-key-for-value1-value2",
                                        "Placeholder",
                                        List.of("value1", "value2"),
                                        null
                                )))
                        ));
    }

    @Test
    public void parsesMultipleKeyMappingsFromMixedArrayForEdgeSourceNode() {
        var sourceNode =
                new JSONObject(
                        Map.of(
                                "source",
                                Map.of(
                                        "label", "Placeholder", "keys", List.of("value1", Map.of("key2", "value2")))));
        var edge =
                new JSONObject(
                        Map.of(
                                "name", "an-edge",
                                "source", "a-source",
                                "mappings", sourceNode));

        NodeTarget result = MappingMapper.parseEdgeNode(sourceNode, "source", WriteMode.MERGE);

        assertThat(result)
                .isEqualTo(
                        new NodeTarget(
                                true,
                                "an-edge-source",
                                "a-source",
                                null,
                                WriteMode.MERGE,
                                null,
                                List.of("Placeholder"),
                                List.of(
                                        new PropertyMapping("value1", "value1", null),
                                        new PropertyMapping("key2", "value2", null)
                                ),
                                nodeKeys(List.of(new NodeKeyConstraint(
                                        "an-edge-source-node-single-key-for-value1",
                                        "Placeholder",
                                        List.of("value1"),
                                        null
                                ), new NodeKeyConstraint(
                                        "an-edge-source-node-key-for-value2",
                                        "Placeholder",
                                        List.of("value2"),
                                        null
                                )))
                        ));
    }

    @Test
    public void parsesLabels() {
        var mappings = Map.<String, Object>of("labels", new String[]{"\"Customer\"", "\"Buyer\""});

        List<String> labels = MappingMapper.parseLabels(new JSONObject(mappings));

        assertThat(labels).isEqualTo(List.of("Customer", "Buyer"));
    }

    @Test
    public void parsesMandatoryMappingArrayOfNodeTarget() {
        var mappings =
                new JSONObject(
                        Map.of(
                                "properties",
                                Map.of("mandatory", List.of(Map.of("source_field", "targetProperty")))));

        NodeSchema nodeSchema =
                MappingMapper.parseNodeSchema("placeholder-target", List.of("Placeholder"), mappings);

        assertThat(nodeSchema)
                .isEqualTo(
                        new NodeSchema(
                                null,
                                null,
                                null,
                                List.of(
                                        new NodeExistenceConstraint(
                                                "placeholder-target-Placeholder-node-not-null-for-targetProperty",
                                                "Placeholder",
                                                "targetProperty")),
                                null,
                                null,
                                null,
                                null,
                                null));
    }

    @Test
    public void parsesMandatoryMappingObjectOfEdgeTarget() {
        var mappings =
                new JSONObject(
                        Map.of("properties", Map.of("mandatory", Map.of("source_field", "targetProperty"))));

        RelationshipSchema schema =
                MappingMapper.parseEdgeSchema("placeholder-target", "PLACEHOLDER", mappings);

        assertThat(schema)
                .isEqualTo(
                        new RelationshipSchema(
                                null,
                                null,
                                null,
                                List.of(
                                        new RelationshipExistenceConstraint(
                                                "placeholder-target-PLACEHOLDER-relationship-not-null-for-targetProperty",
                                                "targetProperty")),
                                null,
                                null,
                                null,
                                null,
                                null));
    }

    @Test
    public void supportsBooleanPropertiesDefinedAsObjectArray() {
        var mappings =
                new JSONObject(
                        Map.of(
                                "properties",
                                Map.of(
                                        "keys",
                                        "placeholder",
                                        "booleans",
                                        List.of(
                                                Map.of("boolean_source_field1", "booleanNodeProperty1"),
                                                Map.of("boolean_source_field2", "booleanNodeProperty2")))));

        List<PropertyMapping> propertyMappings = MappingMapper.parseMappings(mappings);

        assertThat(propertyMappings)
                .isEqualTo(
                        List.of(
                                new PropertyMapping("placeholder", "placeholder", null),
                                new PropertyMapping("boolean_source_field1", "booleanNodeProperty1", BOOLEAN),
                                new PropertyMapping("boolean_source_field2", "booleanNodeProperty2", BOOLEAN)));
    }

    private static NodeSchema nodeKeys(List<NodeKeyConstraint> keys) {
        return new NodeSchema(null, keys, null, null, null, null, null, null, null);
    }
}
