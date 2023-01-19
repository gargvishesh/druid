package io.imply.druid.sql.calcite.functions;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.external.CatalogExternalTableOperatorConversion;

import javax.validation.constraints.NotNull;
import java.util.Collections;
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

  static class PolarisSourceFunctionDefn implements PolarisTableFunctionDefn
  {
    private @NotNull final String source;

    public PolarisSourceFunctionDefn(
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
    public PolarisSourceFunctionDefn convertArgsToTblFnDefn(Map<String, Object> args)
    {
      final String sourceValue = CatalogUtils.getString(args, SOURCE_PARAM);
      if (null == CatalogUtils.getString(args, SOURCE_PARAM)) {
        throw new IAE(StringUtils.format(
            "%s requires a value for the '%s' parameter",
            FUNCTION_NAME,
            SOURCE_PARAM
        ));
      }
      return new PolarisSourceFunctionDefn(sourceValue);
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
