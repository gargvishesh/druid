/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.table.BaseTableFunction;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PolarisSourceFunctionTest
{
  private static final String FUNCTION_NAME = "POLARIS_SOURCE";
  private static final String SOURCE_PARAM = "source";
  private static final String SOURCE_VALUE = "< polaris source >";
  private static final Map<String, Object> ARGS_WITH_SOURCE = ImmutableMap.of(SOURCE_PARAM, SOURCE_VALUE);
  private static final List<ColumnSpec> COLUMNS = Arrays.asList(
      new ColumnSpec("x", Columns.SQL_VARCHAR, null),
      new ColumnSpec("y", Columns.SQL_BIGINT, null)
  );
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private PolarisTableFunctionResolver resolver;
  private PolarisSourceOperatorConversion.PolarisSourceFunction function;

  @Before
  public void setup()
  {
    resolver = Mockito.mock(PolarisTableFunctionResolver.class);
    function = new PolarisSourceOperatorConversion.PolarisSourceFunction(resolver);
  }

  @Test
  public void test_parameters_expected()
  {
    List<TableFunction.ParameterDefn> parameterDefs = function.parameters();
    Assert.assertEquals(1, parameterDefs.size());
    TableFunction.ParameterDefn expectedParameter = new BaseTableFunction.Parameter(
        SOURCE_PARAM,
        TableFunction.ParameterType.VARCHAR,
        false
    );
    Assert.assertEquals(expectedParameter.name(), parameterDefs.get(0).name());
    Assert.assertEquals(expectedParameter.type(), parameterDefs.get(0).type());
    Assert.assertEquals(expectedParameter.isOptional(), parameterDefs.get(0).isOptional());
  }

  @Test
  public void test_convertArgsToTblFnDefn_requiredParamtersGiven_convertsSuccessfully()
  {
    PolarisSourceOperatorConversion.PolarisSourceFunctionSpec functionSpec =
        function.convertArgsToTblFnDefn(ARGS_WITH_SOURCE);

    PolarisSourceOperatorConversion.PolarisSourceFunctionSpec expectedFunctionSpec =
        new PolarisSourceOperatorConversion.PolarisSourceFunctionSpec(SOURCE_VALUE);
    Assert.assertEquals(expectedFunctionSpec, functionSpec);
  }

  @Test
  public void test_convertArgsToTblFnDefn_requiredParamtersMissing_conversionFails()
  {
    IAE exception = Assert.assertThrows(IAE.class, () -> function.convertArgsToTblFnDefn(ImmutableMap.of()));
    Assert.assertTrue(exception.getMessage().contains("requires a value for the"));
    try {
      function.convertArgsToTblFnDefn(ImmutableMap.of());
    }
    catch (IAE e) {
      exception = e;
    }

    Assert.assertNotNull(exception);
    Assert.assertTrue(exception.getMessage().contains("requires a value for the"));
  }

  @Test
  public void test_apply_properArgs_expectedExtTableSpec()
  {
    PolarisTableFunctionSpec tblFnSpec = new PolarisSourceOperatorConversion.PolarisSourceFunctionSpec(SOURCE_VALUE);
    InlineInputSource inputSource = new InlineInputSource("a\nc");
    JsonInputFormat inputFormat = new JsonInputFormat(null, null, null, null, null);
    RowSignature rowSignature = RowSignature.builder()
        .add("col_double", ColumnType.DOUBLE)
        .add("col_float", ColumnType.FLOAT)
        .add("col_long", ColumnType.LONG)
        .add("col_string", ColumnType.STRING)
        .build();
    PolarisExternalTableSpec polarisExtTblSpec = new PolarisExternalTableSpec(inputSource, inputFormat, rowSignature);
    Mockito.when(resolver.resolve(tblFnSpec)).thenReturn(polarisExtTblSpec);

    ExternalTableSpec extTableSpec =
        function.apply(FUNCTION_NAME, ARGS_WITH_SOURCE, null, MAPPER);
    ExternalTableSpec expectedExtTblSpec = new ExternalTableSpec(
        inputSource,
        inputFormat,
        rowSignature,
        () -> Collections.singleton(BasePolarisInputSourceDefn.TYPE_KEY)
    );
    Assert.assertEquals(expectedExtTblSpec.inputSource, extTableSpec.inputSource);
    Assert.assertEquals(expectedExtTblSpec.inputFormat, extTableSpec.inputFormat);
    Assert.assertEquals(expectedExtTblSpec.signature, extTableSpec.signature);
  }

  @Test
  public void test_apply_columnsInExtend_expectedExtTableSpec()
  {
    DruidException exception = Assert.assertThrows(DruidException.class, () -> function.apply(FUNCTION_NAME, ARGS_WITH_SOURCE, COLUMNS, MAPPER));
    Assert.assertTrue(exception.getMessage().contains("does not support the EXTEND clause"));
  }
}
