/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.polaris;

import com.google.common.collect.ImmutableList;
import io.imply.druid.UtilityBeltModule;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PolarisExplainTableOperatorConversionTest extends BaseCalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(new UtilityBeltModule());
  }

  @Test
  public void testSimpleExplainPlanFromPolaris()
  {
    skipVectorize();
    String nonVectorizationResult = "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":\"inline\","
                                    + "\"columnNames\":[\"x\",\"y\",\"z\"],\"columnTypes\":[\"STRING\",\"STRING\","
                                    + "\"LONG\"],\"rows\":[]},\"intervals\":{\"type\":\"intervals\","
                                    + "\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]}"
                                    + ",\"resultFormat\":\"compactedList\",\"columns\":[\"x\",\"y\",\"z\"],"
                                    + "\"legacy\":false,\"context\":{\"defaultTimeout\":300000,"
                                    + "\"maxScatterGatherBytes\":9223372036854775807,"
                                    + "\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\","
                                    + "\"useNativeQueryExplain\":\"true\",\"vectorize\":\"false\","
                                    + "\"vectorizeVirtualColumns\":\"false\"},\"granularity\":{\"type\":\"all\"}},"
                                    + "\"signature\":[{\"name\":\"x\",\"type\":\"STRING\"},"
                                    + "{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}],"
                                    + "\"columnMappings\":[{\"queryColumn\":\"x\",\"outputColumn\":\"x\"},{\"queryColumn\":\"y\",\"outputColumn\":\"y\"},{\"queryColumn\":\"z\",\"outputColumn\":\"z\"}]}]";
    String attributes = "{\"statementType\":\"SELECT\"}";
    Map<String, Object> contextWithNativeExplain = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    contextWithNativeExplain.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, "true");

    testQuery(
        "EXPLAIN PLAN FOR SELECT * FROM "
        + "TABLE(POLARIS_EXPLAIN('[{\"name\": \"x\", \"type\": \"STRING\"}, {\"name\": "
        + "\"y\", \"type\": \"STRING\"}, {\"name\": \"z\", \"type\": \"LONG\"}]'))",
        contextWithNativeExplain,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{nonVectorizationResult, "[]", attributes}
        )
    );
  }

  @Test
  public void testTransformExplainPlanFromPolaris()
  {
    skipVectorize();
    String nonVectorizationResult = "[{\"query\":{\"queryType\":\"scan\",\"dataSource\":{\"type\":"
                                    + "\"inline\",\"columnNames\":[\"x\",\"y\",\"z\"],\"columnTypes\":[\"STRING\","
                                    + "\"STRING\",\"LONG\"],\"rows\":[]},\"intervals\":{\"type\":\"intervals\","
                                    + "\"intervals\":[\"-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z\"]},"
                                    + "\"virtualColumns\":[{\"type\":\"expression\",\"name\":\"v0\",\"expression\":"
                                    + "\"concat(\\\"x\\\",'foo')\",\"outputType\":\"STRING\"},{\"type\":\"expression\","
                                    + "\"name\":\"v1\",\"expression\":\"(\\\"z\\\" + 100)\",\"outputType\":\"LONG\"}],"
                                    + "\"resultFormat\":\"compactedList\",\"columns\":[\"v0\",\"v1\",\"y\"],"
                                    + "\"legacy\":false,\"context\":{\"defaultTimeout\":300000,"
                                    + "\"maxScatterGatherBytes\":9223372036854775807,"
                                    + "\"sqlCurrentTimestamp\":\"2000-01-01T00:00:00Z\",\"sqlQueryId\":\"dummy\","
                                    + "\"useNativeQueryExplain\":\"true\",\"vectorize\":\"false\","
                                    + "\"vectorizeVirtualColumns\":\"false\"},\"granularity\":{\"type\":\"all\"}},"
                                    + "\"signature\":[{\"name\":\"v0\",\"type\":\"STRING\"},{\"name\":\"y\","
                                    + "\"type\":\"STRING\"},{\"name\":\"v1\",\"type\":\"LONG\"}],"
                                    + "\"columnMappings\":[{\"queryColumn\":\"v0\",\"outputColumn\":\"EXPR$0\"},{\"queryColumn\":\"y\",\"outputColumn\":\"y\"},{\"queryColumn\":\"v1\",\"outputColumn\":\"EXPR$2\"}]}]";
    String attributes = "{\"statementType\":\"SELECT\"}";
    Map<String, Object> contextWithNativeExplain = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    contextWithNativeExplain.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, "true");

    testQuery(
        "EXPLAIN PLAN FOR SELECT concat(x, 'foo'), y, z + 100 FROM "
        + "TABLE(POLARIS_EXPLAIN('[{\"name\": \"x\", \"type\": \"STRING\"}, {\"name\": "
        + "\"y\", \"type\": \"STRING\"}, {\"name\": \"z\", \"type\": \"LONG\"}]'))",
        contextWithNativeExplain,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{nonVectorizationResult, "[]", attributes}
        )
    );
  }
}
