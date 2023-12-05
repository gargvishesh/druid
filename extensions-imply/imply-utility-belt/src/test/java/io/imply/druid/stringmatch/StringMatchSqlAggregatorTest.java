/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.stringmatch;

import com.google.common.collect.ImmutableList;
import io.imply.druid.UtilityBeltModule;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.junit.Test;

import java.util.Collections;

public class StringMatchSqlAggregatorTest extends BaseCalciteQueryTest
{
  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModules(new UtilityBeltModule());
  }

  @Test
  public void testStringMatch()
  {
    testQuery(
        "SELECT STRING_MATCH(dim1) FROM foo",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .descending(false)
                  .aggregators(new StringMatchAggregatorFactory("a0", "dim1", null, false))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{NullHandling.defaultStringValue()})
    );
  }

  @Test
  public void testStringMatchGroupBy()
  {
    testQuery(
        "SELECT cnt, STRING_MATCH(dim1), COUNT(*) FROM foo GROUP BY cnt ORDER BY COUNT(*) DESC",
        Collections.singletonList(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("cnt", "d0", ColumnType.LONG))
                        .setAggregatorSpecs(
                            new StringMatchAggregatorFactory("a0", "dim1", null, false),
                            new CountAggregatorFactory("a1")
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "a1",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(new Object[]{1L, NullHandling.defaultStringValue(), 6L})
    );
  }

  @Test
  public void testStringMatchTopN()
  {
    cannotVectorize(); // topN cannot vectorize
    testQuery(
        "SELECT dim1, STRING_MATCH(dim1), MAX(m1) FROM foo GROUP BY dim1 ORDER BY MAX(m1) DESC LIMIT 1",
        Collections.singletonList(
            new TopNQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                .granularity(Granularities.ALL)
                .dimension(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                .aggregators(
                    new StringMatchAggregatorFactory("a0", "dim1", null, false),
                    new FloatMaxAggregatorFactory("a1", "m1")
                )
                .metric("a1")
                .threshold(1)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(new Object[]{"abc", "abc", 6f})
    );
  }

  @Test
  public void testStringMatchGroupBy2()
  {
    testQuery(
        "SELECT dim1, STRING_MATCH(dim1), COUNT(*) FROM foo GROUP BY dim1 ORDER BY COUNT(*) DESC",
        Collections.singletonList(
            GroupByQuery.builder()
                        .setDataSource(CalciteTests.DATASOURCE1)
                        .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                        .setGranularity(Granularities.ALL)
                        .setDimensions(new DefaultDimensionSpec("dim1", "d0", ColumnType.STRING))
                        .setAggregatorSpecs(
                            new StringMatchAggregatorFactory("a0", "dim1", null, false),
                            new CountAggregatorFactory("a1")
                        )
                        .setLimitSpec(
                            new DefaultLimitSpec(
                                ImmutableList.of(
                                    new OrderByColumnSpec(
                                        "a1",
                                        OrderByColumnSpec.Direction.DESCENDING,
                                        StringComparators.NUMERIC
                                    )
                                ),
                                Integer.MAX_VALUE
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"", "", 1L},
            new Object[]{"1", "1", 1L},
            new Object[]{"10.1", "10.1", 1L},
            new Object[]{"2", "2", 1L},
            new Object[]{"abc", "abc", 1L},
            new Object[]{"def", "def", 1L}
        )
    );
  }

  @Test
  public void testStringMatchWithFilter()
  {
    testQuery(
        "SELECT STRING_MATCH(dim1) FROM foo WHERE dim1 = 'abc'",
        Collections.singletonList(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(CalciteTests.DATASOURCE1)
                  .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                  .granularity(Granularities.ALL)
                  .descending(false)
                  .aggregators(new StringMatchAggregatorFactory("a0", "dim1", null, false))
                  .filters(equality("dim1", "abc", ColumnType.STRING))
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(new Object[]{"abc"})
    );
  }
}
