/*
 * Copyright (C) 2021 Google LLC
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

import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.targets.EntityTarget;
import org.neo4j.importer.v1.targets.NodeSchema;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.PropertyMapping;
import org.neo4j.importer.v1.targets.RelationshipSchema;
import org.neo4j.importer.v1.targets.RelationshipTarget;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

/**
 * Generates cypher based on model metadata.
 */
// TODO: Needs to be refactored to use DSL.
public class CypherGenerator {
    private static final String ROWS_VARIABLE_NAME = "rows";
    private static final String ROW_VARIABLE_NAME = "row";

    /**
     * getCypherQuery generates the batch import statement of the specified node or relationship target
     *
     * @param importSpecification the whole import specification
     * @param target              the node or relationship target
     * @return the batch import query
     */
    public static String getImportStatement(ImportSpecification importSpecification, EntityTarget target) {
        var type = target.getTargetType();
        switch (type) {
            case NODE:
                return unwindNodes((NodeTarget) target);
            case RELATIONSHIP:
                return unwindRelationships(importSpecification, (RelationshipTarget) target);
            default:
                throw new IllegalArgumentException(String.format("unexpected target type: %s", type));
        }
    }

    /**
     * Generates the Cypher schema statements for the given target.
     *
     * @return a collection of Cypher schema statements
     */
    public static Set<String> getSchemaStatements(EntityTarget target) {
        var type = target.getTargetType();
        switch (type) {
            case NODE:
                return getNodeSchemaStatements((NodeTarget) target);
            case RELATIONSHIP:
                return getRelationshipSchemaStatements((RelationshipTarget) target);
            default:
                throw new IllegalArgumentException(String.format("unexpected target type: %s", type));
        }
    }

    private static String unwindNodes(NodeTarget nodeTarget) {
        String cypherLabels = CypherPatterns.labels(nodeTarget.getLabels());
        CypherPatterns patterns = CypherPatterns.parsePatterns(nodeTarget, "n", ROW_VARIABLE_NAME);

        return "UNWIND $" + ROWS_VARIABLE_NAME + " AS " + ROW_VARIABLE_NAME + " " +
               nodeTarget.getWriteMode().name() + " (n" + cypherLabels + " {" + patterns.keysPattern() + "}) " + patterns.nonKeysSetClause();
    }

    private static String unwindRelationships(ImportSpecification importSpecification, RelationshipTarget relationship) {
        String nodeClause = relationship.getNodeMatchMode().name();
        NodeTarget startNode = resolveRelationshipNode(importSpecification, relationship.getStartNodeReference());
        CypherPatterns startNodePatterns = CypherPatterns.parsePatterns(startNode, "start", ROW_VARIABLE_NAME);
        NodeTarget endNode = resolveRelationshipNode(importSpecification, relationship.getEndNodeReference());
        CypherPatterns endNodePatterns = CypherPatterns.parsePatterns(endNode, "end", ROW_VARIABLE_NAME);
        String relationshipClause = relationship.getWriteMode().name();
        CypherPatterns relationshipPatterns = CypherPatterns.parsePatterns(relationship, "r", ROW_VARIABLE_NAME);

        return "UNWIND $" + ROWS_VARIABLE_NAME + " AS " + ROW_VARIABLE_NAME + " " +
               nodeClause + "(start " + CypherPatterns.labels(startNode.getLabels()) + " {" + startNodePatterns.keysPattern() + "})" + startNodePatterns.nonKeysSetClause() + " " +
               nodeClause + "(end " + CypherPatterns.labels(endNode.getLabels()) + " {" + endNodePatterns.keysPattern() + "})" + endNodePatterns.nonKeysSetClause() + " " +
               relationshipClause + "(start)-[r:`" + relationship.getType() + "` {" + relationshipPatterns.keysPattern() + "}]->(end) " + relationshipPatterns.nonKeysSetClause();
    }

    private static Set<String> getNodeSchemaStatements(NodeTarget target) {
        NodeSchema schema = target.getSchema();
        Set<String> statements = new LinkedHashSet<>();
        // TODO: node type constraints
        for (var constraint : schema.getNodeKeyConstraints()) {
            statements.add("CREATE CONSTRAINT " + constraint.getName() + " IF NOT EXISTS FOR (n:" + CypherPatterns.escape(constraint.getLabel()) + ") REQUIRE " + CypherPatterns.propertyList("n", constraint.getProperties()) + " IS NODE KEY " + CypherPatterns.schemaOptions(constraint.getOptions()));
        }
        for (var constraint : schema.getNodeUniqueConstraints()) {
            statements.add("CREATE CONSTRAINT " + constraint.getName() + " IF NOT EXISTS FOR (n:" + CypherPatterns.escape(constraint.getLabel()) + ") REQUIRE " + CypherPatterns.propertyList("n", constraint.getProperties()) + " IS UNIQUE " + CypherPatterns.schemaOptions(constraint.getOptions()));
        }
        for (var constraint : schema.getNodeExistenceConstraints()) {
            statements.add("CREATE CONSTRAINT " + constraint.getName() + " IF NOT EXISTS FOR (n:" + CypherPatterns.escape(constraint.getLabel()) + ") REQUIRE n." + CypherPatterns.escape(constraint.getProperty()) + " IS NOT NULL");
        }
        for (var index : schema.getRangeIndexes()) {
            statements.add("CREATE INDEX " + index.getName() + " IF NOT EXISTS FOR (n:" + CypherPatterns.escape(index.getLabel()) + ") ON (" + CypherPatterns.propertyList("n", index.getProperties()) + ")");
        }
        for (var index : schema.getTextIndexes()) {
            statements.add("CREATE TEXT INDEX " + index.getName() + " IF NOT EXISTS FOR (n:" + CypherPatterns.escape(index.getLabel()) + ") ON (n." + CypherPatterns.escape(index.getProperty()) + ") " + CypherPatterns.schemaOptions(index.getOptions()));
        }
        for (var index : schema.getPointIndexes()) {
            statements.add("CREATE POINT INDEX " + index.getName() + " IF NOT EXISTS FOR (n:" + CypherPatterns.escape(index.getLabel()) + ") ON (n." + CypherPatterns.escape(index.getProperty()) + ") " + CypherPatterns.schemaOptions(index.getOptions()));
        }
        for (var index : schema.getFullTextIndexes()) {
            statements.add("CREATE FULLTEXT INDEX " + index.getName() + " IF NOT EXISTS FOR (n:" + CypherPatterns.labels(index.getLabels(), "|") + ") ON EACH [" + CypherPatterns.propertyList("n", index.getProperties()) + "] " + CypherPatterns.schemaOptions(index.getOptions()));
        }
        for (var index : schema.getVectorIndexes()) {
            statements.add("CREATE VECTOR INDEX " + index.getName() + " IF NOT EXISTS FOR (n:" + CypherPatterns.escape(index.getLabel()) + ") ON (n." + CypherPatterns.escape(index.getProperty()) + ") " + CypherPatterns.schemaOptions(index.getOptions()));
        }
        return statements;
    }


    private static Set<String> getRelationshipSchemaStatements(RelationshipTarget target) {
        RelationshipSchema schema = target.getSchema();
        Set<String> statements = new LinkedHashSet<>();
        String type = target.getType();
        // TODO: relation type constraints
        for (var constraint : schema.getRelationshipKeyConstraints()) {
            statements.add("CREATE CONSTRAINT " + constraint.getName() + " IF NOT EXISTS FOR ()-[r:" + CypherPatterns.escape(type) + "]-() REQUIRE " + CypherPatterns.propertyList("r", constraint.getProperties()) + " IS NODE KEY " + CypherPatterns.schemaOptions(constraint.getOptions()));
        }
        for (var constraint : schema.getRelationshipUniqueConstraints()) {
            statements.add("CREATE CONSTRAINT " + constraint.getName() + " IF NOT EXISTS FOR ()-[r:" + CypherPatterns.escape(type) + "]-() REQUIRE " + CypherPatterns.propertyList("r", constraint.getProperties()) + " IS UNIQUE " + CypherPatterns.schemaOptions(constraint.getOptions()));
        }
        for (var constraint : schema.getRelationshipExistenceConstraints()) {
            statements.add("CREATE CONSTRAINT " + constraint.getName() + " IF NOT EXISTS FOR ()-[r:" + CypherPatterns.escape(type) + "]-() REQUIRE r." + CypherPatterns.escape(constraint.getProperty()) + " IS NOT NULL");
        }
        for (var index : schema.getRangeIndexes()) {
            statements.add("CREATE INDEX " + index.getName() + " IF NOT EXISTS FOR ()-[r:" + CypherPatterns.escape(type) + "]-() ON (" + CypherPatterns.propertyList("r", index.getProperties()) + ")");
        }
        for (var index : schema.getTextIndexes()) {
            statements.add("CREATE TEXT INDEX " + index.getName() + " IF NOT EXISTS FOR ()-[r:" + CypherPatterns.escape(type) + "]-() ON (r." + CypherPatterns.escape(index.getProperty()) + ") " + CypherPatterns.schemaOptions(index.getOptions()));
        }
        for (var index : schema.getPointIndexes()) {
            statements.add("CREATE POINT INDEX " + index.getName() + " IF NOT EXISTS FOR ()-[r:" + CypherPatterns.escape(type) + "]-() ON (r." + CypherPatterns.escape(index.getProperty()) + ") " + CypherPatterns.schemaOptions(index.getOptions()));
        }
        for (var index : schema.getFullTextIndexes()) {
            statements.add("CREATE FULLTEXT INDEX " + index.getName() + " IF NOT EXISTS FOR ()-[r:" + CypherPatterns.escape(type) + "]-() ON EACH [" + CypherPatterns.propertyList("r", index.getProperties()) + "] " + CypherPatterns.schemaOptions(index.getOptions()));
        }
        for (var index : schema.getVectorIndexes()) {
            statements.add("CREATE VECTOR INDEX " + index.getName() + " IF NOT EXISTS FOR ()-[r:" + CypherPatterns.escape(type) + "]-() ON (r." + CypherPatterns.escape(index.getProperty()) + ") " + CypherPatterns.schemaOptions(index.getOptions()));
        }
        return statements;
    }

    private static NodeTarget resolveRelationshipNode(ImportSpecification importSpecification, String reference) {
        return importSpecification.getTargets().getNodes().stream()
                .filter(target -> reference.equals(target.getName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Could not resolve node target reference %s", reference)));
    }
}

class CypherPatterns {

    private final String keyPropertiesPattern;
    private final String nonKeyPropertiesSet;

    private CypherPatterns(String keyPropertiesPattern, String nonKeyPropertiesSet) {
        this.keyPropertiesPattern = keyPropertiesPattern;
        this.nonKeyPropertiesSet = nonKeyPropertiesSet;
    }

    public static CypherPatterns parsePatterns(EntityTarget entity, String entityVariable, String rowVariable) {
        Set<String> keyProperties = new LinkedHashSet<>(entity.getKeyProperties());
        String cypherKeyProperties = propertyList(entity, escapeAll(keyProperties), rowVariable);
        List<String> nonKeyProperties = new ArrayList<>(entity.getAllProperties());
        nonKeyProperties.removeAll(keyProperties);
        String cypherSetNonKeys = propertyList(entity, propertyList(entityVariable, nonKeyProperties), rowVariable, "SET ", "=");
        return new CypherPatterns(cypherKeyProperties, cypherSetNonKeys);
    }

    public static Collection<String> propertyList(String variable, List<String> properties) {
        return prefixWith(variable + ".", escapeAll(properties));
    }

    public static String schemaOptions(Map<String, Object> options) {
        return "OPTIONS " + optionsAsMap(options);
    }

    @SuppressWarnings("unchecked")
    private static String schemaOption(Object value) {
        if (value instanceof Map) {
            return optionsAsMap((Map<String, Object>) value);
        }
        if (value instanceof Collection<?>) {
            return optionsAsList((Collection<?>) value);
        }
        if (value instanceof String) {
            return String.format("'%s'", value);
        }
        return String.valueOf(value);
    }

    private static String optionsAsMap(Map<String, Object> options) {
        return options.entrySet()
                .stream()
                .map(entry -> String.format("`%s`: %s", entry.getKey(), schemaOption(entry.getValue())))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private static String optionsAsList(Collection<?> value) {
        return value.stream().map(CypherPatterns::schemaOption).collect(Collectors.joining(",", "[", "]"));
    }

    public String keysPattern() {
        return keyPropertiesPattern;
    }

    public String nonKeysSetClause() {
        return nonKeyPropertiesSet;
    }

    public static String labels(List<String> labels) {
        return labels(labels, ":");
    }

    public static String labels(List<String> labels, String separator) {
        return labels
                .stream()
                .collect(Collectors.joining(String.format("`%s`", separator), ":`", "`"));
    }

    private static String propertyList(EntityTarget target, Collection<String> properties, String rowVariable) {
        return propertyList(target, properties, rowVariable, "", ":");
    }

    private static String propertyList(EntityTarget target, Collection<String> properties, String rowVariable, String prefix, String separator) {
        Map<String, String> fieldsByProperty = target.getProperties().stream().collect(toMap(PropertyMapping::getTargetProperty, PropertyMapping::getSourceField));
        return properties
                .stream()
                .map(property -> String.format("%s%s%s.`%s`", property, separator, rowVariable, fieldsByProperty.get(property)))
                .collect(Collectors.joining(", ", prefix, ""));
    }

    private static Collection<String> prefixWith(String prefix, List<String> elements) {
        return elements.stream().map(element -> String.format("%s%s", prefix, element)).collect(Collectors.toList());
    }

    private static List<String> escapeAll(Collection<String> properties) {
        return properties.stream().map(CypherPatterns::escape).collect(Collectors.toList());
    }

    public static String escape(String identifier) {
        return String.format("`%s`", identifier);
    }
}
