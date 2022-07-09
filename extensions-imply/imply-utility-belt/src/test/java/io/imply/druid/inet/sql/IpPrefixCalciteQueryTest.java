/*
 *
 *  * Copyright (c) Imply Data, Inc. All rights reserved.
 *  *
 *  * This software is the confidential and proprietary information
 *  * of Imply Data, Inc. You shall not disclose such Confidential
 *  * Information and shall use it only in accordance with the terms
 *  * of the license agreement you entered into with Imply.
 *
 *
 */

package io.imply.druid.inet.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.imply.druid.inet.IpAddressModule;
import io.imply.druid.inet.column.IpAddressTestUtils;
import io.imply.druid.inet.column.IpPrefixDimensionSchema;
import io.imply.druid.inet.expression.IpAddressExpressions;
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
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
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

public class IpPrefixCalciteQueryTest extends BaseCalciteQueryTest
{
  private static final String DATA_SOURCE = "iptest";

  private static final DruidOperatorTable OPERATOR_TABLE = new DruidOperatorTable(
      ImmutableSet.of(
          new CountSqlAggregator(new ApproxCountDistinctSqlAggregator(new BuiltinApproxCountDistinctSqlAggregator()))
      ),
      ImmutableSet.of(
          new IpAddressSqlOperatorConversions.PrefixParseOperatorConversion(),
          new IpAddressSqlOperatorConversions.PrefixTryParseOperatorConversion(),
          new IpAddressSqlOperatorConversions.StringifyOperatorConversion(),
          new IpAddressSqlOperatorConversions.MatchOperatorConversion(),
          new IpAddressSqlOperatorConversions.SearchOperatorConversion(),
          new IpAddressSqlOperatorConversions.HostOperatorConversion()
      )
  );

  private static final List<ImmutableMap<String, Object>> RAW_ROWS = ImmutableList.of(
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-01")
          .put("ipv4", "172.14.158.234/16")
          .put("ipv6", "2001:0db8:0000:0000:0000:8a2e:0370:7334/64")
          .put("ipvmix", "6.5.4.3/16")
          .put("string", "aaa")
          .put("long", 5L)
          .build(),
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-01")
          .put("ipv4", "172.14.164.200/16")
          .put("ipv6", "28:7:6:5:5:6:7:8/64")
          .put("ipvmix", "11:22:33:44:55:66:77:88/64")
          .put("string", "bbb")
          .put("long", 4L)
          .build(),
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-01")
          .put("ipv4", "215.235.105.56/16")
          .put("ipv6", "c305:f175:393b:0c09:baed:a3fd:26d2:a0ba/64")
          .put("ipvmix", "100.200.123.12/16")
          .put("string", "ccc")
          .put("long", 3L)
          .build(),
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-01")
          .put("ipv4", "172.14.158.239/16")
          .put("ipv6", "3114:ae86:4484:0347:7d48:1452:55d2:405c/64")
          .put("ipvmix", "3556:7b75:d9b1:ed81:0bc3:cee1:9480:af90/64")
          .put("string", "ddd")
          .put("long", 2L)
          .build(),
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-01")
          .put("ipv4", "109.8.10.192/16")
          .put("ipv6", "890d:941d:cc37:3229:2ece:5e9f:fd53:0073/64")
          .put("ipvmix", "77b7:23dc:0aca:5c74:5148:cc23:103e:af9b/64")
          .put("string", "eee")
          .put("long", 1L)
          .build(),
      // repeat on another day
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-02")
          .put("ipv4", "172.14.158.234/16")
          .put("ipv6", "2001:0db8:0000:0000:0000:8a2e:0370:7334/64")
          .put("ipvmix", "6.5.4.3/16")
          .put("string", "aaa")
          .put("long", 5L)
          .build(),
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-02")
          .put("ipv4", "172.14.164.200/16")
          .put("ipv6", "28:7:6:5:5:6:7:8/64")
          .put("ipvmix", "11:22:33:44:55:66:77:88/64")
          .put("string", "bbb")
          .put("long", 4L)
          .build(),
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-02")
          .put("ipv4", "215.235.105.56/16")
          .put("ipv6", "c305:f175:393b:0c09:baed:a3fd:26d2:a0ba/64")
          .put("ipvmix", "100.200.123.12/16")
          .put("string", "ccc")
          .put("long", 3L)
          .build(),
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-02")
          .put("ipv4", "172.14.158.239/16")
          .put("ipv6", "3114:ae86:4484:0347:7d48:1452:55d2:405c/64")
          .put("ipvmix", "3556:7b75:d9b1:ed81:0bc3:cee1:9480:af90/64")
          .put("string", "ddd")
          .put("long", 2L)
          .build(),
      ImmutableMap.<String, Object>builder()
          .put("t", "2000-01-02")
          .put("ipv4", "109.8.10.192/16")
          .put("ipv6", "890d:941d:cc37:3229:2ece:5e9f:fd53:0073/64")
          .put("ipvmix", "77b7:23dc:0aca:5c74:5148:cc23:103e:af9b/64")
          .put("string", "eee")
          .put("long", 1L)
          .build()
  );

  private static final InputRowParser<Map<String, Object>> PARSER = new MapInputRowParser(
      new TimeAndDimsParseSpec(
          new TimestampSpec("t", "iso", null),
          DimensionsSpec.builder().setDimensions(
              ImmutableList.<DimensionSchema>builder()
                  .add(new StringDimensionSchema("string"))
                  .add(new IpPrefixDimensionSchema("ipv4", true))
                  .add(new IpPrefixDimensionSchema("ipv6", true))
                  .add(new IpPrefixDimensionSchema("ipvmix", true))
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
    exprMacros.add(new IpAddressExpressions.PrefixParseExprMacro());
    exprMacros.add(new IpAddressExpressions.PrefixTryParseExprMacro());
    exprMacros.add(new IpAddressExpressions.StringifyExprMacro());
    exprMacros.add(new IpAddressExpressions.MatchExprMacro());
    exprMacros.add(new IpAddressExpressions.SearchExprMacro());
    exprMacros.add(new IpAddressExpressions.HostExprMacro());
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
        + "IP_STRINGIFY(ipv4, 0), "
        + "IP_STRINGIFY(ipv6, 0), "
        + "IP_STRINGIFY(ipvmix, 0), "
        + "IP_STRINGIFY(IP_HOST(ipv4)), "
        + "IP_STRINGIFY(IP_HOST(ipv6)), "
        + "IP_STRINGIFY(IP_HOST(ipvmix)), "
        + "SUM(cnt) "
        + "FROM druid.iptest GROUP BY 1,2,3,4,5,6,7,8,9",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn("v0", "ip_stringify(\"ipv4\")", ColumnType.STRING, macroTable),
                            new ExpressionVirtualColumn("v1", "ip_stringify(\"ipv6\")", ColumnType.STRING, macroTable),
                            new ExpressionVirtualColumn("v2", "ip_stringify(\"ipvmix\")", ColumnType.STRING, macroTable),
                            new ExpressionVirtualColumn("v3", "ip_stringify(\"ipv4\",0)", ColumnType.STRING, macroTable),
                            new ExpressionVirtualColumn("v4", "ip_stringify(\"ipv6\",0)", ColumnType.STRING, macroTable),
                            new ExpressionVirtualColumn("v5", "ip_stringify(\"ipvmix\",0)", ColumnType.STRING, macroTable),
                            new ExpressionVirtualColumn("v6", "ip_stringify(ip_host(\"ipv4\"))", ColumnType.STRING, macroTable),
                            new ExpressionVirtualColumn("v7", "ip_stringify(ip_host(\"ipv6\"))", ColumnType.STRING, macroTable),
                            new ExpressionVirtualColumn("v8", "ip_stringify(ip_host(\"ipvmix\"))", ColumnType.STRING, macroTable)
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
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "109.8.10.192/16",
                "890d:941d:cc37:3229:2ece:5e9f:fd53:73/64",
                "77b7:23dc:aca:5c74:5148:cc23:103e:af9b/64",
                "109.8.10.192/16",
                "890d:941d:cc37:3229:2ece:5e9f:fd53:0073/64",
                "77b7:23dc:0aca:5c74:5148:cc23:103e:af9b/64",
                "109.8.10.192",
                "890d:941d:cc37:3229:2ece:5e9f:fd53:73",
                "77b7:23dc:aca:5c74:5148:cc23:103e:af9b",
                2L
            },
            new Object[]{
                "172.14.158.234/16",
                "2001:db8::8a2e:370:7334/64",
                "6.5.4.3/16",
                "172.14.158.234/16",
                "2001:0db8:0000:0000:0000:8a2e:0370:7334/64",
                "6.5.4.3/16",
                "172.14.158.234",
                "2001:db8::8a2e:370:7334",
                "6.5.4.3",
                2L
            },
            new Object[]{
                "172.14.158.239/16",
                "3114:ae86:4484:347:7d48:1452:55d2:405c/64",
                "3556:7b75:d9b1:ed81:bc3:cee1:9480:af90/64",
                "172.14.158.239/16",
                "3114:ae86:4484:0347:7d48:1452:55d2:405c/64",
                "3556:7b75:d9b1:ed81:0bc3:cee1:9480:af90/64",
                "172.14.158.239",
                "3114:ae86:4484:347:7d48:1452:55d2:405c",
                "3556:7b75:d9b1:ed81:bc3:cee1:9480:af90",
                2L
            },
            new Object[]{
                "172.14.164.200/16",
                "28:7:6:5:5:6:7:8/64",
                "11:22:33:44:55:66:77:88/64",
                "172.14.164.200/16",
                "0028:0007:0006:0005:0005:0006:0007:0008/64",
                "0011:0022:0033:0044:0055:0066:0077:0088/64",
                "172.14.164.200",
                "28:7:6:5:5:6:7:8",
                "11:22:33:44:55:66:77:88",
                2L
            },
            new Object[]{
                "215.235.105.56/16",
                "c305:f175:393b:c09:baed:a3fd:26d2:a0ba/64",
                "100.200.123.12/16",
                "215.235.105.56/16",
                "c305:f175:393b:0c09:baed:a3fd:26d2:a0ba/64",
                "100.200.123.12/16",
                "215.235.105.56",
                "c305:f175:393b:c09:baed:a3fd:26d2:a0ba",
                "100.200.123.12",
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
        + "FROM druid.iptest WHERE IP_MATCH('172.14.158.0', ipv4) GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn("v0", "ip_stringify(\"ipv4\")", ColumnType.STRING, macroTable)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(expressionFilter("ip_match('172.14.158.0',\"ipv4\")"))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "172.14.158.234/16",
                2L
            },
            new Object[]{
                "172.14.158.239/16",
                2L
            },
            new Object[]{
                "172.14.164.200/16",
                2L
            }
        )
    );
  }

  @Test
  public void testGroupByFormatFilterSearchWithoutDot() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "IP_STRINGIFY(ipv4), "
        + "SUM(cnt) "
        + "FROM druid.iptest WHERE IP_SEARCH('17', ipv4) GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn("v0", "ip_stringify(\"ipv4\")", ColumnType.STRING, macroTable)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(expressionFilter("ip_search('17',\"ipv4\")"))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "172.14.158.234/16",
                2L
            },
            new Object[]{
                "172.14.158.239/16",
                2L
            },
            new Object[]{
                "172.14.164.200/16",
                2L
            }
        )
    );
  }

  @Test
  public void testGroupByFormatFilterSearchWithDot() throws Exception
  {
    cannotVectorize();
    testQuery(
        "SELECT "
        + "IP_STRINGIFY(ipv4), "
        + "SUM(cnt) "
        + "FROM druid.iptest WHERE IP_SEARCH('172.14.', ipv4) GROUP BY 1",
        ImmutableList.of(
            GroupByQuery.builder()
                        .setDataSource(DATA_SOURCE)
                        .setInterval(querySegmentSpec(Filtration.eternity()))
                        .setGranularity(Granularities.ALL)
                        .setVirtualColumns(
                            new ExpressionVirtualColumn("v0", "ip_stringify(\"ipv4\")", ColumnType.STRING, macroTable)
                        )
                        .setDimensions(
                            dimensions(
                                new DefaultDimensionSpec("v0", "d0")
                            )
                        )
                        .setDimFilter(expressionFilter("ip_search('172.14.',\"ipv4\")"))
                        .setAggregatorSpecs(aggregators(new LongSumAggregatorFactory("a0", "cnt")))
                        .setContext(QUERY_CONTEXT_DEFAULT)
                        .build()
        ),
        ImmutableList.of(
            new Object[]{
                "172.14.158.234/16",
                2L
            },
            new Object[]{
                "172.14.158.239/16",
                2L
            },
            new Object[]{
                "172.14.164.200/16",
                2L
            }
        )
    );
  }
}
