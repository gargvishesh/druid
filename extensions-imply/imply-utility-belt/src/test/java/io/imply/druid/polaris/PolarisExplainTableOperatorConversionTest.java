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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PolarisExplainTableOperatorConversionTest extends BaseCalciteQueryTest
{
  @Override
  public DruidOperatorTable createOperatorTable()
  {
    return new DruidOperatorTable(
        ImmutableSet.of(),
        ImmutableSet.of(
            new PolarisExplainTableOperatorConversion(new PolarisExplainTableMacro(new DefaultObjectMapper()))
        )
    );
  }

  @Test
  public void testSimpleExplainPlanFromPolaris() throws Exception
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
                                    + "{\"name\":\"y\",\"type\":\"STRING\"},{\"name\":\"z\",\"type\":\"LONG\"}]}]";
    Map<String, Object> contextWithNativeExplain = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    contextWithNativeExplain.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, "true");
    testQuery(
        "EXPLAIN PLAN FOR SELECT * FROM "
        + "TABLE(POLARIS_EXPLAIN('[{\"name\": \"x\", \"type\": \"STRING\"}, {\"name\": "
        + "\"y\", \"type\": \"STRING\"}, {\"name\": \"z\", \"type\": \"LONG\"}]'))",
        contextWithNativeExplain,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{nonVectorizationResult, "[]"}

        )
    );
  }

  @Test
  public void testTransformExplainPlanFromPolaris() throws Exception
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
                                    + "\"type\":\"STRING\"},{\"name\":\"v1\",\"type\":\"LONG\"}]}]";
    Map<String, Object> contextWithNativeExplain = new HashMap<>(QUERY_CONTEXT_DEFAULT);
    contextWithNativeExplain.put(PlannerConfig.CTX_KEY_USE_NATIVE_QUERY_EXPLAIN, "true");
    testQuery(
        "EXPLAIN PLAN FOR SELECT concat(x, 'foo'), y, z + 100 FROM "
        + "TABLE(POLARIS_EXPLAIN('[{\"name\": \"x\", \"type\": \"STRING\"}, {\"name\": "
        + "\"y\", \"type\": \"STRING\"}, {\"name\": \"z\", \"type\": \"LONG\"}]'))",
        contextWithNativeExplain,
        ImmutableList.of(),
        ImmutableList.of(
            new Object[]{nonVectorizationResult, "[]"}

        )
    );
  }
}
