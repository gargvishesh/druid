/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumn;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ViewDefinitionValidationUtils
{
  private static final Logger LOG = new Logger(ViewDefinitionValidationUtils.class);

  private static final Pattern DRUID_QUERY_REL_PATTERN =
      Pattern.compile("^\\s*DruidQueryRel\\(query=\\[(.*)\\], signature=\\[.*\\]\\)$");

  private static final Pattern DRUID_OUTER_QUERY_REL_PATTERN =
      Pattern.compile("^\\s*DruidOuterQueryRel\\(query=\\[(.*)\\], signature=\\[.*\\]\\)$");

  private static final Pattern DRUID_JOIN_QUERY_REL_PATTERN =
      Pattern.compile("^\\s*DruidJoinQueryRel\\(.*, signature=\\[.*\\]\\)$");

  private static final Pattern DRUID_UNION_DATASOURCE_REL_PATTERN =
      Pattern.compile("^\\s*DruidUnionDataSourceRel\\(.*, signature=\\[.*\\]\\)$");

  private static final Pattern DRUID_UNION_REL_PATTERN =
      Pattern.compile("^\\s*DruidUnionRel\\(.*\\)$");

  private static final Pattern BINDABLE_REL_PATTERN =
      Pattern.compile("^\\s*Bindable.*\\(.*\\)$");

  public static String getQueryPlanFromExplainResponse(String explainResponseContent, ObjectMapper jsonMapper)
  {
    try {
      List<Map<String, Object>> responseMap = jsonMapper.readValue(
          explainResponseContent,
          new TypeReference<List<Map<String, Object>>>()
          {
          }
      );

      if (responseMap.size() != 1) {
        throw new InternalValidationException(
            "Explain response should only have one entity, response[%s]",
            explainResponseContent
        );
      }

      String queryPlan = (String) responseMap.get(0).get("PLAN");
      if (queryPlan == null) {
        throw new InternalValidationException(
            "Null PLAN when validating view definition, response[%s]",
            explainResponseContent
        );
      }
      return queryPlan;
    }
    catch (JsonProcessingException jpe) {
      throw new InternalValidationException(
          jpe,
          "Could not deserialize query plan from EXPLAIN PLAN FOR response[%s]",
          explainResponseContent
      );
    }
  }

  private static void validateSingleLevelDruidQuery(Query druidQuery)
  {
    // Query must be non-aggregating, so it must be a ScanQuery
    if (!(druidQuery instanceof ScanQuery)) {
      throw new ClientValidationException("Aggregations cannot be used in view definitions.");
    }
    ScanQuery scanQuery = (ScanQuery) druidQuery;

    // Check what the query is reading from, we only support base datasources
    if ((scanQuery.getDataSource() instanceof LookupDataSource)) {
      throw new ClientValidationException("Lookup datasources cannot be used in view definitions.");
    }
    if ((scanQuery.getDataSource() instanceof InlineDataSource)) {
      // Inline datasources are not currently supported by Druid SQL, so this should never happen
      throw new ClientValidationException("Inline datasources cannot be used in view definitions.");
    }
    if (!(scanQuery.getDataSource() instanceof TableDataSource)) {
      // generic fallback check for non-Table datasources, but join/union/subquery should have been caught earlier
      // based on checking the outermost rel type and number of rels
      // TBD: do we support GlobalTableDataSource in view definitions?
      throw new ClientValidationException(
          "Only queries on Druid datasources can be used as view definitions, query[%s]",
          scanQuery
      );
    }

    if (scanQuery.getOrder() != ScanQuery.Order.NONE) {
      throw new ClientValidationException("ORDER BY cannot be used in view definitions.");
    }

    if (scanQuery.getScanRowsOffset() != 0) {
      throw new ClientValidationException("OFFSET cannot be used in view definitions.");
    }

    if (scanQuery.getScanRowsLimit() != Long.MAX_VALUE) {
      throw new ClientValidationException("LIMIT cannot be used in view definitions.");
    }

    // no transforms on projected columns
    for (String column : scanQuery.getColumns()) {
      VirtualColumn virtualColumn = scanQuery.getVirtualColumns().getVirtualColumn(column);
      if (virtualColumn != null) {
        // If the required columns are empty, this virtual column is a constant expression, which we allow to support
        // cases such as the one shown in ViewDefinitionValidationUtils.test_columnReplacedByConstantExpression_allowed
        // where a column being selected is replaced by the constant expression
        if (!virtualColumn.requiredColumns().isEmpty()) {
          throw new ClientValidationException(
              "Transformations cannot be applied to projected columns in view definitions, columns%s",
              virtualColumn.requiredColumns()
          );
        }
      }
    }
  }

  public static void validateQueryPlan(String queryPlan, ObjectMapper jsonMapper)
  {
    String[] queryRels = queryPlan.split("\n");

    if (queryRels.length < 1) {
      // this should never happen
      throw new InternalValidationException(
          "Got empty queryRels while validating view definition query plan[%s]",
          queryPlan
      );
    }

    String outermostQueryRel = queryRels[0];
    Matcher queryMatcher = DRUID_QUERY_REL_PATTERN.matcher(outermostQueryRel);

    if (queryRels.length == 1 && queryMatcher.matches()) {
      // we should have only one DruidQueryRel, potentially good case, validate further
      String serializedQuery = queryMatcher.group(1);
      try {
        Query query = jsonMapper.readValue(serializedQuery, Query.class);
        validateSingleLevelDruidQuery(query);
      }
      catch (JsonProcessingException jpe) {
        throw new InternalValidationException(
            jpe,
            "Could not deserialize Query object from query rel [%s], plan[%s]",
            outermostQueryRel,
            queryPlan
        );
      }
    } else {
      // unsupported query type, get more details
      if (DRUID_OUTER_QUERY_REL_PATTERN.matcher(outermostQueryRel).matches()) {
        throw new ClientValidationException("Subqueries cannot be used in view definitions.");
      }
      if (DRUID_JOIN_QUERY_REL_PATTERN.matcher(outermostQueryRel).matches()) {
        throw new ClientValidationException("Joins or subqueries in the WHERE clause cannot be used in view definitions.");
      }
      if (DRUID_UNION_DATASOURCE_REL_PATTERN.matcher(outermostQueryRel).matches()) {
        throw new ClientValidationException("UNION ALL cannot be used in view definitions.");
      }
      if (DRUID_UNION_REL_PATTERN.matcher(outermostQueryRel).matches()) {
        throw new ClientValidationException("UNION ALL cannot be used in view definitions.");
      }
      if (BINDABLE_REL_PATTERN.matcher(outermostQueryRel).matches()) {
        throw new ClientValidationException("Only queries on Druid datasources can be used as view definitions.");
      }
      // If none of the other matchers triggered, we have multiple levels of DruidQueryRel
      if (queryRels.length > 1) {
        throw new ClientValidationException("Nested queries cannot be used in view definitions.");
      }

      // We got a single level query but rel type is unrecognized, this should not happen
      throw new InternalValidationException("Unsupported view definition, plan[%s]", queryPlan);
    }
  }

  /**
   * Indicates user error in the provided view definition
   */
  public static class ClientValidationException extends RuntimeException
  {
    public ClientValidationException(String formatText, Object... arguments)
    {
      super(StringUtils.nonStrictFormat(formatText, arguments));
    }
  }

  /**
   * Indicates probable non-user error, perhaps a result of an internal failure or bug
   */
  public static class InternalValidationException extends RuntimeException
  {
    public InternalValidationException(String formatText, Object... arguments)
    {
      super(StringUtils.nonStrictFormat(formatText, arguments));
    }

    public InternalValidationException(Throwable t, String formatText, Object... arguments)
    {
      super(StringUtils.nonStrictFormat(formatText, arguments), t);
    }
  }
}
