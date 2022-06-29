/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.imply.druid.inet.IpAddressModule;
import io.imply.druid.inet.column.IpAddressDimensionSchema;
import io.imply.druid.inet.column.IpAddressTestUtils;
import io.imply.druid.inet.expression.IpAddressExpressions;
import io.imply.druid.inet.segment.virtual.IpAddressFormatVirtualColumn;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimeAndDimsParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.LookupExprMacro;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.aggregation.ApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.BuiltinApproxCountDistinctSqlAggregator;
import org.apache.druid.sql.calcite.aggregation.builtin.CountSqlAggregator;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IpAddressCalciteQueryTest extends BaseCalciteQueryTest
{
  private static final String DATA_SOURCE = "iptest";

  private static final DruidOperatorTable OPERATOR_TABLE = new DruidOperatorTable(
      ImmutableSet.of(
          new CountSqlAggregator(new ApproxCountDistinctSqlAggregator(new BuiltinApproxCountDistinctSqlAggregator()))
      ),
      ImmutableSet.of(
          new IpAddressSqlOperatorConversions.AddressParseOperatorConversion(),
          new IpAddressSqlOperatorConversions.AddressTryParseOperatorConversion(),
          new IpAddressSqlOperatorConversions.StringifyOperatorConversion(),
          new IpAddressSqlOperatorConversions.PrefixOperatorConversion(),
          new IpAddressSqlOperatorConversions.MatchOperatorConversion()
      )
  );

  private static final List<ImmutableMap<String, Object>> RAW_ROWS = ImmutableList.of(
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("ipv4", "172.14.158.234")
                  .put("ipv6", "2001:0db8:0000:0000:0000:8a2e:0370:7334")
                  .put("ipvmix", "6.5.4.3")
                  .put("string", "aaa")
                  .put("long", 5L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("ipv4", "172.14.164.200")
                  .put("ipv6", "28:7:6:5:5:6:7:8")
                  .put("ipvmix", "11:22:33:44:55:66:77:88")
                  .put("string", "bbb")
                  .put("long", 4L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("ipv4", "215.235.105.56")
                  .put("ipv6", "c305:f175:393b:0c09:baed:a3fd:26d2:a0ba")
                  .put("ipvmix", "100.200.123.12")
                  .put("string", "ccc")
                  .put("long", 3L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("ipv4", "172.14.158.239")
                  .put("ipv6", "3114:ae86:4484:0347:7d48:1452:55d2:405c")
                  .put("ipvmix", "3556:7b75:d9b1:ed81:0bc3:cee1:9480:af90")
                  .put("string", "ddd")
                  .put("long", 2L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-01")
                  .put("ipv4", "109.8.10.192")
                  .put("ipv6", "890d:941d:cc37:3229:2ece:5e9f:fd53:0073")
                  .put("ipvmix", "77b7:23dc:0aca:5c74:5148:cc23:103e:af9b")
                  .put("string", "eee")
                  .put("long", 1L)
                  .build(),
      // repeat on another day
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("ipv4", "172.14.158.234")
                  .put("ipv6", "2001:0db8:0000:0000:0000:8a2e:0370:7334")
                  .put("ipvmix", "6.5.4.3")
                  .put("string", "aaa")
                  .put("long", 5L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("ipv4", "172.14.164.200")
                  .put("ipv6", "28:7:6:5:5:6:7:8")
                  .put("ipvmix", "11:22:33:44:55:66:77:88")
                  .put("string", "bbb")
                  .put("long", 4L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("ipv4", "215.235.105.56")
                  .put("ipv6", "c305:f175:393b:0c09:baed:a3fd:26d2:a0ba")
                  .put("ipvmix", "100.200.123.12")
                  .put("string", "ccc")
                  .put("long", 3L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("ipv4", "172.14.158.239")
                  .put("ipv6", "3114:ae86:4484:0347:7d48:1452:55d2:405c")
                  .put("ipvmix", "3556:7b75:d9b1:ed81:0bc3:cee1:9480:af90")
                  .put("string", "ddd")
                  .put("long", 2L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("ipv4", "109.8.10.192")
                  .put("ipv6", "890d:941d:cc37:3229:2ece:5e9f:fd53:0073")
                  .put("ipvmix", "77b7:23dc:0aca:5c74:5148:cc23:103e:af9b")
                  .put("string", "eee")
                  .put("long", 1L)
                  .build(),
      ImmutableMap.<String, Object>builder()
                  .put("t", "2000-01-02")
                  .put("string", "fff")
                  .put("long", 1L)
                  .build()
  );

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec("t", "iso", null),
          DimensionsSpec.builder().setDimensions(
              ImmutableList.<DimensionSchema>builder()
                           .add(new StringDimensionSchema("string"))
                           .add(new IpAddressDimensionSchema("ipv4", true))
                           .add(new IpAddressDimensionSchema("ipv6", true))
                           .add(new IpAddressDimensionSchema("ipvmix", true))
                           .add(new LongDimensionSchema("long"))
                           .build()
          ).build()
      ));

  private static final List<InputRow> ROWS =
      RAW_ROWS.stream().map(raw -> CalciteTests.createRow(raw, PARSER)).collect(Collectors.toList());

  private ExprMacroTable macroTable;


  @Override
  public Iterable<? extends Module> getJacksonModules()
  {
    return Iterables.concat(
        super.getJacksonModules(),
        IpAddressTestUtils.LICENSED_IP_ADDRESS_MODULE.getJacksonModules()
    );
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker createQuerySegmentWalker() throws IOException
  {
    IpAddressModule.registerHandlersAndSerde();
    this.macroTable = createMacroTable();
    final QueryableIndex index =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt")
                            )
                            .withDimensionsSpec(PARSER)
                            .withRollup(false)
                            .build()
                    )
                    .rows(ROWS)
                    .buildMMappedIndex();

    return new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
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
    return OPERATOR_TABLE;
  }

  @Override
  public ExprMacroTable createMacroTable()
  {
    final List<ExprMacroTable.ExprMacro> exprMacros = new ArrayList<>();
    for (Class<? extends ExprMacroTable.ExprMacro> clazz : ExpressionModule.EXPR_MACROS) {
      exprMacros.add(CalciteTests.INJECTOR.getInstance(clazz));
    }
    exprMacros.add(CalciteTests.INJECTOR.getInstance(LookupExprMacro.class));
    exprMacros.add(new IpAddressExpressions.AddressParseExprMacro());
    exprMacros.add(new IpAddressExpressions.AddressTryParseExprMacro());
    exprMacros.add(new IpAddressExpressions.StringifyExprMacro());
    exprMacros.add(new IpAddressExpressions.PrefixExprMacro());
    exprMacros.add(new IpAddressExpressions.MatchExprMacro());
    return new ExprMacroTable(exprMacros);
  }

  @Test
  public void testGroupByFormat() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "IP_STRINGIFY(ipv4), "
        + "IP_STRINGIFY(ipv6), "
        + "IP_STRINGIFY(ipvmix), "
        + "IP_STRINGIFY(ipv4, false), "
        + "IP_STRINGIFY(ipv6, false), "
        + "IP_STRINGIFY(ipvmix, false), "
        + "IP_STRINGIFY(IP_PREFIX(ipv4, 16)), "
        + "IP_STRINGIFY(IP_PREFIX(ipv6, 16)), "
        + "IP_STRINGIFY(IP_PREFIX(ipvmix, 16)), "
        + "IP_PARSE('1.2.3.4'), "
        + "SUM(cnt) "
        + "FROM druid.iptest GROUP BY 1,2,3,4,5,6,7,8,9",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new IpAddressFormatVirtualColumn("v0", "ipv4", true, false),
                            new IpAddressFormatVirtualColumn("v1", "ipv6", true, false),
                            new IpAddressFormatVirtualColumn("v2", "ipvmix", true, false),
                            new IpAddressFormatVirtualColumn("v3", "ipv4", false, false),
                            new IpAddressFormatVirtualColumn("v4", "ipv6", false, false),
                            new IpAddressFormatVirtualColumn("v5", "ipvmix", false, false),
                            new ExpressionVirtualColumn(
                                "v6",
                                "ip_stringify(ip_prefix(\"ipv4\",16))",
                                ColumnType.STRING,
                                macroTable
                            ),
                            new ExpressionVirtualColumn(
                                "v7",
                                "ip_stringify(ip_prefix(\"ipv6\",16))",
                                ColumnType.STRING,
                                macroTable
                            ),
                            new ExpressionVirtualColumn(
                                "v8",
                                "ip_stringify(ip_prefix(\"ipvmix\",16))",
                                ColumnType.STRING,
                                macroTable
                            )
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0"),
                                new DefaultDimensionSpec("v1", "d1"),
                                new DefaultDimensionSpec("v2", "d2"),
                                new DefaultDimensionSpec("v3", "d3"),
                                new DefaultDimensionSpec("v4", "d4"),
                                new DefaultDimensionSpec("v5", "d5"),
                                new DefaultDimensionSpec("v6", "d6"),
                                new DefaultDimensionSpec("v7", "d7"),
                                new DefaultDimensionSpec("v8", "d8")
                            )
                        )
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new ExpressionPostAggregator(
                                    "p0",
                                    "ip_parse('1.2.3.4')",
                                    null,
                                    macroTable
                                )
                            )
                        )
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                NullHandling.defaultStringValue(),
                "\"AAAAAAAAAAAAAP//AQIDBA==\"",
                1L
            },
            new Object[]{
                "109.8.10.192",
                "890d:941d:cc37:3229:2ece:5e9f:fd53:73",
                "77b7:23dc:aca:5c74:5148:cc23:103e:af9b",
                "109.8.10.192",
                "890d:941d:cc37:3229:2ece:5e9f:fd53:0073",
                "77b7:23dc:0aca:5c74:5148:cc23:103e:af9b",
                "109.8.0.0",
                "890d::",
                "77b7::",
                "\"AAAAAAAAAAAAAP//AQIDBA==\"",
                2L
            },
            new Object[]{
                "172.14.158.234",
                "2001:db8::8a2e:370:7334",
                "6.5.4.3",
                "172.14.158.234",
                "2001:0db8:0000:0000:0000:8a2e:0370:7334",
                "6.5.4.3",
                "172.14.0.0",
                "2001::",
                "6.5.0.0",
                "\"AAAAAAAAAAAAAP//AQIDBA==\"",
                2L
            },
            new Object[]{
                "172.14.158.239",
                "3114:ae86:4484:347:7d48:1452:55d2:405c",
                "3556:7b75:d9b1:ed81:bc3:cee1:9480:af90",
                "172.14.158.239",
                "3114:ae86:4484:0347:7d48:1452:55d2:405c",
                "3556:7b75:d9b1:ed81:0bc3:cee1:9480:af90",
                "172.14.0.0",
                "3114::",
                "3556::",
                "\"AAAAAAAAAAAAAP//AQIDBA==\"",
                2L
            },
            new Object[]{
                "172.14.164.200",
                "28:7:6:5:5:6:7:8",
                "11:22:33:44:55:66:77:88",
                "172.14.164.200",
                "0028:0007:0006:0005:0005:0006:0007:0008",
                "0011:0022:0033:0044:0055:0066:0077:0088",
                "172.14.0.0",
                "28::",
                "11::",
                "\"AAAAAAAAAAAAAP//AQIDBA==\"",
                2L
            },
            new Object[]{
                "215.235.105.56",
                "c305:f175:393b:c09:baed:a3fd:26d2:a0ba",
                "100.200.123.12",
                "215.235.105.56",
                "c305:f175:393b:0c09:baed:a3fd:26d2:a0ba",
                "100.200.123.12",
                "215.235.0.0",
                "c305::",
                "100.200.0.0",
                "\"AAAAAAAAAAAAAP//AQIDBA==\"",
                2L
            }
        )
    );
  }

  @Test
  public void testGroupByFormatFilterMatch() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "IP_STRINGIFY(ipv4), "
        + "SUM(cnt) "
        + "FROM druid.iptest WHERE IP_MATCH(ipv4, '172.14.158.0/24') GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new IpAddressFormatVirtualColumn("v0", "ipv4", true, false)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(expressionFilter("ip_match(\"ipv4\",'172.14.158.0/24')"))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "172.14.158.234",
                2L
            },
            new Object[]{
                "172.14.158.239",
                2L
            }
        )
    );
  }

  @Test
  public void testGroupByFormatSelectorFilter() throws Exception
  {
    testQuery(
        "SELECT "
        + "IP_STRINGIFY(ipv4), "
        + "SUM(cnt) "
        + "FROM druid.iptest WHERE IP_STRINGIFY(ipv4) = '172.14.164.200' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new IpAddressFormatVirtualColumn("v0", "ipv4", true, false)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(selector("v0", "172.14.164.200", null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "172.14.164.200",
                2L
            }
        )
    );
  }

  @Test
  public void testGroupByFormatSelectorFilterNull() throws Exception
  {
    testQuery(
        "SELECT "
        + "string, "
        + "SUM(cnt) "
        + "FROM druid.iptest WHERE IP_STRINGIFY(ipv4) IS NOT NULL GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new IpAddressFormatVirtualColumn("v0", "ipv4", true, false)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("string", "d0")
                            )
                        )
                        .setDimFilter(not(selector("v0", null, null)))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{"aaa", 2L},
            new Object[]{"bbb", 2L},
            new Object[]{"ccc", 2L},
            new Object[]{"ddd", 2L},
            new Object[]{"eee", 2L}
        )
    );
  }

  @Test
  public void testGroupByFormatBoundFilter() throws Exception
  {
    testQuery(
        "SELECT "
        + "IP_STRINGIFY(ipv4), "
        + "SUM(cnt) "
        + "FROM druid.iptest WHERE IP_STRINGIFY(ipv4) < '172.14.164.255' AND IP_STRINGIFY(ipv4) > '172.14.158.235' GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new IpAddressFormatVirtualColumn("v0", "ipv4", true, false)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(bound("v0", "172.14.158.235", "172.14.164.255", true, true, null, null))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "172.14.158.239",
                2L
            },
            new Object[]{
                "172.14.164.200",
                2L
            }
        )
    );
  }

  @Test
  public void testTimeseriesStringifyVirtualColumn() throws Exception
  {
    testQuery(
        "SELECT COUNT(DISTINCT IP_STRINGIFY(ipv4)) FROM druid.iptest",
        ImmutableList.of(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(querySegmentSpec(Filtration.eternity()))
                  .granularity(Granularities.ALL)
                  .virtualColumns(
                      new IpAddressFormatVirtualColumn("v0", "ipv4", true, false)
                  )
                  .aggregators(
                      new CardinalityAggregatorFactory(
                          "a0",
                          null,
                          ImmutableList.of(DefaultDimensionSpec.of("v0")),
                          false,
                          true
                      )
                  )
                  .context(QUERY_CONTEXT_DEFAULT)
                  .build()
        ),
        ImmutableList.of(
            new Object[]{
                NullHandling.replaceWithDefault() ? 6L : 5L
            }
        )
    );
  }
}
