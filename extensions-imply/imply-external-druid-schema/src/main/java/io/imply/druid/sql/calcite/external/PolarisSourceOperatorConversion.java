/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.external.DruidUserDefinedTableMacroConversion;

import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Registers the "POLARIS_SOURCE" operator, which is used in queries like
 * <pre>{@code
 * INSERT INTO dst SELECT * FROM TABLE(POLARIS_SOURCE(
 *   '<job source>'))
 *
 * INSERT INTO dst SELECT * FROM TABLE(POLARIS_SOURCE(
 *   source => '<job source>'))
 * }</pre>
 * Where either the by-position or by-name forms are usable.
 */
public class PolarisSourceOperatorConversion extends DruidUserDefinedTableMacroConversion
{
  public static final String FUNCTION_NAME = "POLARIS_SOURCE";

  public static final String SOURCE_PARAM = "source";

  static class PolarisSourceFunctionSpec implements PolarisTableFunctionSpec
  {
    public static final String FUNCTION_NAME = PolarisSourceOperatorConversion.FUNCTION_NAME;
    private @NotNull final String source;

    @JsonCreator
    public PolarisSourceFunctionSpec(
        @JsonProperty("source") @NotNull String source)
    {
      this.source = source;
    }

    @Nonnull
    @JsonProperty("source")
    public String getSource()
    {
      return source;
    }

    @Override
    public String toString()
    {
      return "PolarisSourceFunctionSpec{" +
             "name='" + FUNCTION_NAME + '\'' +
             ", source=" + source +
             '}';
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
      PolarisSourceFunctionSpec that = (PolarisSourceFunctionSpec) o;
      return Objects.equals(source, that.source);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(source);
    }
  }

  /**
   * The use of a table function allows the user to specify the complete Polaris JobSource,
   * which includes a description of the source, format, and row signature.
   */
  static class PolarisSourceFunction extends PolarisTableFunction
  {
    public PolarisSourceFunction(final PolarisTableFunctionResolver resolver)
    {
      super(resolver,
            Collections.singletonList(new Parameter(SOURCE_PARAM, ParameterType.VARCHAR, false))
      );
    }

    @Override
    public PolarisSourceFunctionSpec convertArgsToTblFnDefn(Map<String, Object> args)
    {
      final String sourceValue = CatalogUtils.getString(args, SOURCE_PARAM);
      if (null == sourceValue) {
        throw new IAE(StringUtils.format(
            "%s requires a value for the '%s' parameter",
            FUNCTION_NAME,
            SOURCE_PARAM
        ));
      }
      return new PolarisSourceFunctionSpec(sourceValue);
    }

    @Override
    public ExternalTableSpec apply(
        String fnName,
        Map<String, Object> args,
        List<ColumnSpec> columns,
        ObjectMapper jsonMapper
    )
    {
      if (null != columns) {
        throw new IAE(StringUtils.format(
            "%s does not support the EXTEND clause.",
            FUNCTION_NAME
        ));
      }
      final PolarisTableFunctionSpec tblFnSpec = convertArgsToTblFnDefn(args);
      // expect polaris to return PolarisExternalTableSpec with input, format, and signature non-null
      final PolarisExternalTableSpec polarisExtTblSpec = resolver.resolve(tblFnSpec);

      return new ExternalTableSpec(
          polarisExtTblSpec.getInputSource(),
          polarisExtTblSpec.getInputFormat(),
          polarisExtTblSpec.getSignature());
    }
  }

  @Inject
  public PolarisSourceOperatorConversion(
      @Json final ObjectMapper jsonMapper,
      final PolarisTableFunctionResolver resolver
  )
  {
    super(FUNCTION_NAME, new PolarisSourceFunction(resolver), jsonMapper);
  }
}
