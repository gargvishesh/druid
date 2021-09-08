/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.databind.Module;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.license.TestingImplyLicenseManager;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class ArrayOfDoublesSketchAdTechAggregationTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();
  private final AggregationTestHelper helper;

  public ArrayOfDoublesSketchAdTechAggregationTest(final GroupByQueryConfig config)
  {
    DruidModule module = new ArrayOfDoublesSketchModule();
    ArrayOfDoublesSketchAdTechModule adTechModule = new ArrayOfDoublesSketchAdTechModule();
    module.configure(null);
    adTechModule.configure(null);
    adTechModule.setImplyLicenseManager(new TestingImplyLicenseManager(null));
    List<Module> modules = Stream.concat(module.getJacksonModules().stream(), adTechModule.getJacksonModules().stream())
                                 .collect(Collectors.toList());
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(modules, config, tempFolder);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @After
  public void teardown() throws IOException
  {
    helper.close();
  }

  @Test
  public void testAdTechImpressionCountSketchAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass()
                     .getClassLoader()
                     .getResource("tuple/array_of_doubles_build_data_three_values_and_nulls.tsv")
                     .getFile()),
        String.join(
            "\n",
            "{",
            "  \"type\": \"string\",",
            "  \"parseSpec\": {",
            "    \"format\": \"tsv\",",
            "    \"timestampSpec\": {\"column\": \"timestamp\", \"format\": \"yyyyMMddHH\"},",
            "    \"dimensionsSpec\": {",
            "      \"dimensions\": [\"product\", \"key\"],",
            "      \"dimensionExclusions\": [],",
            "      \"spatialDimensions\": []",
            "    },",
            "    \"columns\": [\"timestamp\", \"product\", \"key\", \"value1\", \"value2\", \"value3\"]",
            "  }",
            "}"
        ),
        String.join(
            "\n",
            "[",
            "  {\"type\": \"doubleSum\", \"name\": \"value1\", \"fieldName\": \"value1\"},",
            "  {\"type\": \"doubleSum\", \"name\": \"value2\", \"fieldName\": \"value2\"},",
            "  {\"type\": \"doubleSum\", \"name\": \"value3\", \"fieldName\": \"value3\"}",
            "]"
        ),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        String.join(
            "\n",
            "{",
            "  \"queryType\": \"groupBy\",",
            "  \"dataSource\": \"test_datasource\",",
            "  \"granularity\": \"ALL\",",
            "  \"dimensions\": [],",
            "  \"aggregations\": [",
            "   {\"type\": \"adTechInventory\", \"name\": \"adTechSketch\", \"userColumn\": \"product\", \"impressionColumn\" : \"value2\", \"frequencyCap\" : 2},",
            "   {\"type\": \"adTechInventory\", \"name\": \"adTechSketch1\", \"userColumn\": \"product\", \"impressionColumn\" : \"value2\"}",
            "  ],",
            "  \"intervals\": [\"2015-01-01T00:00:00.000Z/2015-01-31T00:00:00.000Z\"]",
            "}"
        )
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals("adTechSketch", 18.0, (double) row.get(0), 0);
    Assert.assertEquals("adTechSketch1", 80.0, (double) row.get(1), 0);
  }
}
