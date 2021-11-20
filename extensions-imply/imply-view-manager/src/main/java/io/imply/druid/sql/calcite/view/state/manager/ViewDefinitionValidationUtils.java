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
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class provides utility methods for checking that a view definition has a shape that is supported by Imply.
 *
 * We only support simple view definitions that only project and filter
 * (e.g. SELECT colA, colB FROM tbl WHERE colC='value')
 *
 * The following query features are not supported in view definitions:
 * - Subqueries of any kind
 * - Joins of any kind
 * - UNION ALL
 * - Aggregations
 * - LIMIT, OFFSET, ORDER BY
 * - Queries on anything that is not a base datasource: lookups, INFORMATION_SCHEMA, sys.* tables, other views
 */
public class ViewDefinitionValidationUtils
{
  private static final Pattern DRUID_QUERY_REL_PATTERN =
      Pattern.compile("^\\s*DruidQueryRel\\(query=\\[(.*)], signature=\\[.*]\\)$");

  private static final Pattern DRUID_OUTER_QUERY_REL_PATTERN =
      Pattern.compile("^\\s*DruidOuterQueryRel\\(.*\\)$");

  private static final Pattern DRUID_JOIN_QUERY_REL_PATTERN =
      Pattern.compile("^\\s*DruidJoinQueryRel\\(.*\\)$");

  private static final Pattern DRUID_UNION_DATASOURCE_REL_PATTERN =
      Pattern.compile("^\\s*DruidUnionDataSourceRel\\(.*\\)$");

  private static final Pattern DRUID_UNION_REL_PATTERN =
      Pattern.compile("^\\s*DruidUnionRel\\(.*\\)$");

  private static final Pattern BINDABLE_REL_PATTERN =
      Pattern.compile("^\\s*Bindable.*\\(.*\\)$");

  /**
   * The response returned by Druid for EXPLAIN PLAN FOR queries has the following structure:
   * [
   *  {
   *    "PLAN":"<plan-string>",
   *    "RESOURCES:"<resource-set>"
   *  }
   * ]
   *
   * where the 'plan-string' has the form shown below:
   *
   * DruidQueryRel(query=[<query-json>],signature=[<type-signature-json>])
   *
   * There can be several QueryRel entries in the response, one per line, with indentation indicating
   * query level (such as when there are subqueries).
   *
   * The 'resource-set' contains the datasources and/or views that would be accessed by a query.
   *
   * We do not do anything with the type signature currently.
   *
   * Given the explain response content as a String, this method extracts the 'plan-string' and 'resource-set',
   * which would later be processed by {@link #validateQueryPlanAndResources}.
   *
   * @param explainResponseContent Response content from an EXPLAIN PLAN FOR query
   * @param jsonMapper JSON object mapper
   * @return The plan string and resources from the response
   */
  public static QueryPlanAndResources getQueryPlanAndResourcesFromExplainResponse(String explainResponseContent, ObjectMapper jsonMapper)
  {
    try {
      List<Map<String, Object>> deserializedResponse = jsonMapper.readValue(
          explainResponseContent,
          new TypeReference<List<Map<String, Object>>>()
          {
          }
      );

      if (deserializedResponse.size() != 1) {
        throw new InternalValidationException(
            "Explain response should only have one entity, response[%s]",
            explainResponseContent
        );
      }

      String queryPlan = (String) deserializedResponse.get(0).get("PLAN");
      if (queryPlan == null) {
        throw new InternalValidationException(
            "Null PLAN when validating view definition, response[%s]",
            explainResponseContent
        );
      }

      String resourceSetSerialized = (String) deserializedResponse.get(0).get("RESOURCES");
      Set<Resource> resources = jsonMapper.readValue(
          resourceSetSerialized,
          new TypeReference<Set<Resource>>()
          {
          }
      );
      return new QueryPlanAndResources(queryPlan, resources);
    }
    catch (JsonProcessingException jpe) {
      throw new InternalValidationException(
          jpe,
          "Could not deserialize query plan or resource set from EXPLAIN PLAN FOR response[%s]",
          explainResponseContent
      );
    }
  }

  /**
   * This method checks a single Druid Query object to see if it meets the following criteria:
   * - No aggregations (must be a ScanQuery, which is the only non-aggregating query type that
   *                    can be generated from SQL queries)
   * - Must query a table datasource (no lookups, inline).
   * - No LIMIT, OFFSET, or ORDER BY
   * - No transforms on the projected columns
   *
   * @param druidQuery
   */
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

    if (!scanQuery.getOrderBys().isEmpty()) {
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

  /**
   * This method validates a QueryPlanAndResources (which can be extracted from a response using
   * {@link #getQueryPlanAndResourcesFromExplainResponse}), and checks that:
   * - the view definition does not use any other views
   * - there are no subqueries, joins, or UNION ALL queries.
   *
   * If those checks pass, then {@link #validateSingleLevelDruidQuery} is called for further validation.
   *
   * If validation fails, this method will throw either a {@link ClientValidationException} for errors that are
   * most likely the result of user error, or {@link InternalValidationException} for errors that are likely a result
   * of bugs or internal server errors.
   *
   * @param queryPlanAndResources A QueryPlanAndResources extracted from an EXPLAIN PLAN FOR response
   * @param jsonMapper JSON object mapper
   */
  public static void validateQueryPlanAndResources(QueryPlanAndResources queryPlanAndResources, ObjectMapper jsonMapper)
  {
    for (Resource resource : queryPlanAndResources.getResources()) {
      if (ResourceType.VIEW.equals(resource.getType())) {
        throw new ClientValidationException(
            "View [%s] cannot be used within another view definition.",
            resource.getName()
        );
      }
    }

    String queryPlan = queryPlanAndResources.getQueryPlan();
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

  /**
   * A holder class for the query plan string and set of resources extracted from an EXPLAIN PLAN FOR response.
   */
  public static class QueryPlanAndResources
  {
    private final String queryPlan;
    private final Set<Resource> resources;

    public QueryPlanAndResources(
        String queryPlan,
        Set<Resource> resources
    )
    {
      this.queryPlan = queryPlan;
      this.resources = resources;
    }

    public String getQueryPlan()
    {
      return queryPlan;
    }

    public Set<Resource> getResources()
    {
      return resources;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      QueryPlanAndResources that = (QueryPlanAndResources) o;
      return Objects.equals(getQueryPlan(), that.getQueryPlan()) &&
             Objects.equals(getResources(), that.getResources());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(getQueryPlan(), getResources());
    }
  }
}
