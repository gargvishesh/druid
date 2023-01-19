/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */


package io.imply.druid.sql.calcite.functions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.external.CatalogExternalTableOperatorConversion;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
public class PolarisSourceOperatorConversion extends CatalogExternalTableOperatorConversion
{
  public static final String FUNCTION_NAME = "POLARIS_SOURCE";

  public static final String SOURCE_PARAM = "source";

  static class PolarisSourceFunctionSpec implements PolarisTableFunctionSpec
  {
    private @NotNull final String source;

    public PolarisSourceFunctionSpec(
        @JsonProperty("source") @NotNull String source)
    {
      this.source = source;
    }

    @JsonProperty("name")
    public String getName()
    {
      return FUNCTION_NAME;
    }

    @JsonProperty("source")
    public String getSource()
    {
      return source;
    }
  }

  /**
   * The use of a table function allows the use of optional arguments,
   * so that the signature can be given either as the original-style
   * serialized JSON signature, or the updated SQL-style EXTEND clause.
   */
  private static class PolarisSourceFunction extends PolarisTableFunction
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
      if (null == CatalogUtils.getString(args, SOURCE_PARAM)) {
        throw new IAE(StringUtils.format(
            "%s requires a value for the '%s' parameter",
            FUNCTION_NAME,
            SOURCE_PARAM
        ));
      }
      return new PolarisSourceFunctionSpec(sourceValue);
    }

    @Override
    public String name()
    {
      return PolarisSourceOperatorConversion.FUNCTION_NAME;
    }

    @Override
    public String columnsDefnUnspecifiedError()
    {
      return StringUtils.format(
          "%s requires either that the 'inputSchema' be defined in the '%s' parameter, or an EXTEND clause",
          FUNCTION_NAME,
          SOURCE_PARAM
      );
    }

    @Override
    public String columnsDefnCollisionErrorStr()
    {
      return StringUtils.format(
          "%s requires either that the 'inputSchema' be defined in the '%s' parameter, or an EXTEND clause, but not both",
          FUNCTION_NAME,
          SOURCE_PARAM
      );
    }

    @Override
    public ExternalTableSpec apply(
        String fnName,
        Map<String, Object> args,
        List<ColumnSpec> columns,
        ObjectMapper jsonMapper
    )
    {
      final PolarisTableFunctionSpec tblFnDefn = convertArgsToTblFnDefn(args);
      final PolarisExternalTableSpec polarisExtTblSpec = resolver.resolve(tblFnDefn);

      if (null == columns && null == polarisExtTblSpec.getSignature()) {
        throw new IAE(columnsDefnUnspecifiedError());
      }
      if (null != columns && null != polarisExtTblSpec.getSignature()) {
        throw new IAE(columnsDefnCollisionErrorStr());
      }

      return null == columns ?
             new ExternalTableSpec(
                 polarisExtTblSpec.getInputSource(),
                 // this can technically be nullable but should not be when this function is used.
                 polarisExtTblSpec.getInputFormat(),
                 polarisExtTblSpec.getSignature()) :
             new ExternalTableSpec(
                 polarisExtTblSpec.getInputSource(),
                 polarisExtTblSpec.getInputFormat(),
                 Columns.convertSignature(columns)
             );
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
