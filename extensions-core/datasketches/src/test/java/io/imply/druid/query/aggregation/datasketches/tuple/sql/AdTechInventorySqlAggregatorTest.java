/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.aggregation.datasketches.tuple.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.imply.druid.license.TestingImplyLicenseManager;
import io.imply.druid.query.aggregation.datasketches.tuple.AdTechInventoryAggregatorFactory;
import io.imply.druid.query.aggregation.datasketches.tuple.ImplyArrayOfDoublesSketchModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.tuple.ArrayOfDoublesSketchModule;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class AdTechInventorySqlAggregatorTest extends BaseCalciteQueryTest
{
  private static final String NAME = AdTechInventorySqlAggregator.NAME;

  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    ImplyArrayOfDoublesSketchModule module = new ImplyArrayOfDoublesSketchModule();
    module.setImplyLicenseManager(new TestingImplyLicenseManager(null));
    return Iterables.concat(super.getJacksonModules(), module.getJacksonModules());
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker() throws IOException
  {
    /*
    Layout of DATASOURCE1
    dim1          m1            ...
    ===============================
    -             1.0           ...
    10.1          2.0           ...
    2             3.0           ...
    1             4.0           ...
    def           5.0           ...
    abc           6.0           ...
     */
    new ArrayOfDoublesSketchModule().configure(null);
    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                                         new ArrayOfDoublesSketchAggregatorFactory(
                                                             "comp_dim_1",
                                                             "dim1",
                                                             null,
                                                             ImmutableList.of("m1"),
                                                             null
                                                         )
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(CalciteTests.ROWS1)
                                             .buildMMappedIndex();

    return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(CalciteTests.DATASOURCE1)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );
  }

  @Override
  public DruidOperatorTable createOperatorTable()
  {
    return new DruidOperatorTable(ImmutableSet.of(new AdTechInventorySqlAggregator()), ImmutableSet.of());
  }

  @Test
  public void testAdTechInventorySqlAggregator() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT\n"
        + "  " + NAME + "(dim1, m1),\n"                  // Simple call to the function
        + "  " + NAME + "(dim1, m1, 3),\n"               // Simple call to the function with frequency cap
        + "  " + NAME + "(dim1, m1, 4, 200000),\n"       // Simple call to the function with frequency cap and sample sz
        + "  " + NAME + "(dim1 || 'baz', m1 * 4),\n"      // Call to the function with virtual columns
        + "  " + NAME + "(comp_dim_1, 1)\n"               // Simple call to the function with frequency cap
        + "FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .descending(false)
                  .virtualColumns(
                      new ExpressionVirtualColumn(
                          "v0",
                          "concat(\"dim1\",'baz')",
                          ColumnType.STRING,
                          TestExprMacroTable.INSTANCE
                      ),
                      new ExpressionVirtualColumn(
                          "v1",
                          "(\"m1\" * 4)",
                          ColumnType.DOUBLE,
                          TestExprMacroTable.INSTANCE
                      )
                  )
                  .aggregators(ImmutableList.of(
                      AdTechInventoryAggregatorFactory.getAdTechImpressionCountAggregatorFactory(
                          "a0:agg",
                          "dim1",
                          "m1",
                          null,
                          null,
                          null
                      ),
                      AdTechInventoryAggregatorFactory.getAdTechImpressionCountAggregatorFactory(
                          "a1:agg",
                          "dim1",
                          "m1",
                          null,
                          3,
                          null
                      ),
                      AdTechInventoryAggregatorFactory.getAdTechImpressionCountAggregatorFactory(
                          "a2:agg",
                          "dim1",
                          "m1",
                          null,
                          4,
                          200000
                      ),
                      AdTechInventoryAggregatorFactory.getAdTechImpressionCountAggregatorFactory(
                          "a3:agg",
                          "v0",
                          "v1",
                          null,
                          null,
                          null
                      ),
                      AdTechInventoryAggregatorFactory.getAdTechImpressionCountAggregatorFactory(
                          "a4:agg",
                          null,
                          null,
                          "comp_dim_1",
                          1,
                          null
                      )
                  ))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{
            "20.0",     // 2 + 3 + 4 + 5 + 6
            "14.0",     // 2 + 3 + 3 + 3 + 3
            "17.0",     // 2 + 3 + 4 + 4 + 4
            "84.0",     // 1 + 2 + 3 + 4 + 5 + 6 (first row becomes non empty on concat)
            "5.0"      // 1 + 1 + 1 + 1 + 1
        })
    );
  }
}
