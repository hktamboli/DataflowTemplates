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
package com.google.cloud.teleport.v2.neo4j.utils;

import com.google.cloud.teleport.v2.neo4j.model.enums.FragmentType;
import com.google.cloud.teleport.v2.neo4j.model.enums.RoleType;
import com.google.cloud.teleport.v2.neo4j.model.job.Aggregation;
import com.google.cloud.teleport.v2.neo4j.model.job.Mapping;
import com.google.cloud.teleport.v2.neo4j.model.job.Transform;
import com.google.common.collect.ImmutableList;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.importer.v1.ImportSpecification;
import org.neo4j.importer.v1.sources.Source;
import org.neo4j.importer.v1.targets.CustomQueryTarget;
import org.neo4j.importer.v1.targets.NodeTarget;
import org.neo4j.importer.v1.targets.RelationshipTarget;
import org.neo4j.importer.v1.targets.SourceTransformations;
import org.neo4j.importer.v1.targets.Target;
import org.neo4j.importer.v1.targets.TargetType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility functions for Beam rows and schema.
 */
public class ModelUtils {
    public static final String DEFAULT_STAR_QUERY = "SELECT * FROM PCOLLECTION";
    private static final String neoIdentifierDisAllowedCharactersRegex = "[^a-zA-Z0-9_]";
    private static final String nonAlphaCharsRegex = "[^a-zA-Z]";
    private static final Pattern variablePattern = Pattern.compile("(\\$([a-zA-Z0-9_]+))");
    private static final Logger LOG = LoggerFactory.getLogger(ModelUtils.class);

    public static String getRelationshipKeyField(Target target, FragmentType fragmentType) {
        return getFirstField(target, fragmentType, ImmutableList.of(RoleType.key));
    }

    public static String getFirstField(
            Target target, FragmentType fragmentType, List<RoleType> roleTypes) {
        List<String> fields = getFields(fragmentType, roleTypes, target);
        if (!fields.isEmpty()) {
            return fields.get(0);
        }
        return "";
    }

    public static boolean targetsHaveTransforms(ImportSpecification jobSpec, Source source) {
        for (Target target : jobSpec.getTargets().getAll()) {
            if (!target.isActive()) {
                continue;
            }
            if (!target.getSource().equals(source.getName())) {
                continue;
            }
            if (targetHasTransforms(target)) {
                return true;
            }
        }
        return false;
    }

    public static boolean targetHasTransforms(Target target) {
        if (target.getTargetType() == TargetType.QUERY) {
            return false;
        }
        var sourceTransformations = getSourceTransformations(target);
        if (sourceTransformations == null) {
            return false;
        }
        return sourceTransformations.isEnableGrouping() ||
               !sourceTransformations.getAggregations().isEmpty() ||
               !sourceTransformations.getOrderByClauses().isEmpty()
               || StringUtils.isNotEmpty(sourceTransformations.getWhereClause());
    }

    public static Set<String> getBeamFieldSet(Schema schema) {
        return new HashSet<>(schema.getFieldNames());
    }

    public static String getTargetSql(
            Target target, Set<String> fieldNameMap, boolean generateSqlSort) {
        return getTargetSql(target, fieldNameMap, generateSqlSort, null);
    }

    public static String getTargetSql(
            Target target, Set<String> fieldNameMap,
            boolean generateSqlSort,
            String baseSql) {

        try {
            PlainSelect statement = new PlainSelect();

            statement.withFromItem(new Table("PCOLLECTION"));

            if (generateSqlSort) {
                String sortField = null;
                if (target.getTargetType() == TargetType.RELATIONSHIP) {
                    String keyField = getRelationshipKeyField(target, FragmentType.target);
                    sortField = StringUtils.isNotBlank(keyField) ? escapeField(keyField) : null;
                }

                if (StringUtils.isBlank(sortField)
                    && StringUtils.isNotBlank(target.getTransform().getOrderBy())) {
                    sortField = target.getTransform().getOrderBy();
                }

                if (StringUtils.isNotBlank(sortField)) {
                    statement.withOrderByElements(
                            List.of(
                                    new OrderByElement()
                                            .withExpression(CCJSqlParserUtil.parseExpression(sortField))));
                }
            }

            if (target.getTransform() != null) {
                /////////////////////////////////
                // Grouping transform
                Transform query = target.getTransform();
                if (query.isGroup() || !query.getAggregations().isEmpty()) {
                    Set<String> groupByFields = new LinkedHashSet<>();
                    for (int i = 0; i < target.getMappings().size(); i++) {
                        Mapping mapping = target.getMappings().get(i);
                        if (StringUtils.isNotBlank(mapping.getField())) {
                            if (fieldNameMap.contains(mapping.getField())) {
                                groupByFields.add(mapping.getField());
                            }
                        }
                    }
                    if (groupByFields.isEmpty()) {
                        throw new RuntimeException(
                                "Could not find mapped fields for target: "
                                + target.getName()
                                + ". Please verify that target fields exist in source query.");
                    }
                    statement.addSelectItems(
                            groupByFields.stream().map(s -> new Column(escapeField(s))).toArray(Column[]::new));
                    if (!query.getAggregations().isEmpty()) {
                        for (Aggregation agg : query.getAggregations()) {
                            statement.addSelectItem(
                                    CCJSqlParserUtil.parseExpression(agg.getExpression()),
                                    new Alias(escapeField(agg.getField())));
                        }
                    }

                    if (StringUtils.isNotBlank(query.getWhere())) {
                        statement.withWhere(CCJSqlParserUtil.parseExpression(query.getWhere()));
                    }

                    groupByFields.forEach(
                            s -> statement.addGroupByColumnReference(new Column(escapeField(s))));

                    if (query.getLimit() > -1) {
                        statement.setLimit(new Limit().withRowCount(new LongValue(query.getLimit())));
                    }
                }
            }

            if (statement.getSelectItems() == null || statement.getSelectItems().isEmpty()) {
                statement.addSelectItems(new AllColumns());
            }

            String statementText = statement.toString();
            if (StringUtils.isNotBlank(baseSql)) {
                statementText = statementText.replace("PCOLLECTION", String.format("(%s)", baseSql));
            }
            return statementText;
        } catch (JSQLParserException e) {
            throw new RuntimeException(e);
        }
    }

    private static String escapeField(String keyField) {
        return String.format("`%s`", keyField);
    }

    public static String makeValidNeo4jIdentifier(String proposedIdString) {
        if (isQuoted(proposedIdString)) {
            return proposedIdString;
        }
        String finalIdString =
                proposedIdString.trim().replaceAll(neoIdentifierDisAllowedCharactersRegex, "_");
        if (finalIdString.substring(0, 1).matches(nonAlphaCharsRegex)) {
            finalIdString = "_" + finalIdString;
        }
        return finalIdString;
    }

    public static String makeSpaceSafeValidNeo4jIdentifier(String proposedIdString) {
        proposedIdString = makeValidNeo4jIdentifier(proposedIdString);
        return backTickedExpressionWithSpaces(proposedIdString);
    }

    public static List<String> makeSpaceSafeValidNeo4jIdentifiers(List<String> proposedIds) {
        List<String> safeList = new ArrayList<>();
        for (String proposed : proposedIds) {
            safeList.add(makeSpaceSafeValidNeo4jIdentifier(proposed));
        }
        return safeList;
    }

    private static String backTickedExpressionWithSpaces(String expression) {
        if (expression.indexOf(" ") == -1) {
            return expression;
        }
        String trExpression = expression.trim();
        if (trExpression.startsWith("`")
            || trExpression.startsWith("\"")
            || trExpression.startsWith("'")) {
            // already starts with quotes...
            // NOTE_TODO: strip existing quotes, replace with double quotes
        } else {
            trExpression = "`" + trExpression.trim() + "`";
        }
        return trExpression;
    }

    public static boolean isQuoted(String expression) {
        String trExpression = expression.trim();
        return (trExpression.startsWith("\"") || trExpression.startsWith("'"))
               && (trExpression.endsWith("\"") || trExpression.endsWith("'"));
    }

    // Make relationships idendifiers upper case, no spaces

    public static String makeValidNeo4jRelationshipTypeIdentifier(String proposedTypeIdString) {
        String finalIdString =
                proposedTypeIdString
                        .replaceAll(neoIdentifierDisAllowedCharactersRegex, "_")
                        .toUpperCase()
                        .trim();
        if (!finalIdString.substring(0, 1).matches(nonAlphaCharsRegex)) {
            finalIdString = "N" + finalIdString;
        }
        return finalIdString;
    }
    public static List<String> getStaticOrDynamicRelationshipType(
            String dynamicRowPrefix, Target target) {
        List<String> relationships = new ArrayList<>();
        for (Mapping m : target.getMappings()) {
            if (m.getFragmentType() == FragmentType.rel) {
                if (m.getRole() == RoleType.type) {
                    if (StringUtils.isNotEmpty(m.getConstant())) {
                        relationships.add(m.getConstant());
                    } else {
                        // TODO: handle dynamic labels
                        relationships.add(dynamicRowPrefix + "." + m.getField());
                    }
                }
            }
        }
        if (relationships.isEmpty()) {
            // if relationship labels are not defined, use target name
            relationships.add(ModelUtils.makeValidNeo4jRelationshipTypeIdentifier(target.getName()));
        }
        return relationships;
    }

    public static List<String> getStaticLabels(FragmentType fragmentType, Target target) {
        List<String> labels = new ArrayList<>();
        for (Mapping m : target.getMappings()) {
            if (m.getFragmentType() != fragmentType || m.getRole() != RoleType.label) {
                continue;
            }
            String labelConstantValue = m.getConstant();
            if (StringUtils.isNotEmpty(labelConstantValue)) {
                labels.add(labelConstantValue);
            }
        }
        return labels;
    }

    public static String getStaticType(Target target) {
        for (Mapping m : target.getMappings()) {
            if (m.getFragmentType() != FragmentType.rel) {
                continue;
            }
            if (m.getRole() == RoleType.type) {
                if (StringUtils.isNotEmpty(m.getConstant())) {
                    return m.getConstant();
                }
            }
        }
        throw new IllegalArgumentException(
                String.format(
                        "could not find rel-type definition in the mapping of the relationship target: %s",
                        target));
    }

    public static List<String> getStaticOrDynamicLabels(
            String dynamicRowPrefix, FragmentType entityType, Target target) {
        List<String> labels = new ArrayList<>();
        for (Mapping m : target.getMappings()) {
            if (m.getFragmentType() == entityType) {
                if (m.getRole() == RoleType.label) {
                    if (StringUtils.isNotEmpty(m.getConstant())) {
                        labels.add(m.getConstant());
                    } else {
                        labels.add(dynamicRowPrefix + "." + m.getField());
                    }
                }
            }
        }
        return labels;
    }

    public static List<String> getFields(
            FragmentType entityType, List<RoleType> roleTypes, Target target) {
        List<String> fieldNames = new ArrayList<>();
        for (Mapping m : target.getMappings()) {
            if (m.getFragmentType() == entityType) {
                if (roleTypes.contains(m.getRole())) {
                    fieldNames.add(m.getField());
                }
            }
        }
        return fieldNames;
    }

    public static String replaceVariableTokens(String text, Map<String, String> replacements) {
        Matcher matcher = variablePattern.matcher(text);
        // populate the replacements map ...
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find()) {
            LOG.info("Matcher group: " + matcher.group(1));
            String replacement = replacements.get(matcher.group(2));
            builder.append(text, i, matcher.start());
            if (replacement == null) {
                builder.append(matcher.group(1));
            } else {
                builder.append(replacement);
            }
            i = matcher.end();
        }
        builder.append(text.substring(i));
        String replacedText = builder.toString();
        LOG.info("Before: " + text + ", after: " + replacedText);
        return replacedText;
    }

    public static Set<String> filterProperties(Target target, Predicate<Mapping> mappingPredicate) {
        return target.getMappings().stream()
                .filter(mappingPredicate)
                .map(Mapping::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static SourceTransformations getSourceTransformations(Target target) {
        SourceTransformations sourceTransformations;
        if (target instanceof NodeTarget) {
            sourceTransformations = ((NodeTarget) target).getSourceTransformations();
        } else if (target instanceof RelationshipTarget) {
            sourceTransformations = ((RelationshipTarget) target).getSourceTransformations();
        } else {
            throw new IllegalArgumentException(String.format("Unsupported target type: %s", target.getClass()));
        }
        return sourceTransformations;
    }
}
