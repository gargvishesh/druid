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
import com.google.common.base.Strings;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.BaseTableFunction;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PolarisS3ConnectionInputSourceDefn extends BasePolarisInputSourceDefn
{
  public static final String CONNECTION_NAME_PARAMETER = "connectionName";
  public static final String URIS_PARAMETER = "uris";
  public static final String PREFIXES_PARAMETER = "prefixes";
  public static final String OBJECTS_PARAMETER = "objects";
  public static final String PATTERN_PARAMETER = "pattern";

  private static final TableFunction.ParameterDefn CONNECTION_NAME_PARAM_DEFN = new BaseTableFunction.Parameter(CONNECTION_NAME_PARAMETER, TableFunction.ParameterType.VARCHAR, false);
  private static final TableFunction.ParameterDefn URI_PARAM_DEFN = new BaseTableFunction.Parameter(URIS_PARAMETER, TableFunction.ParameterType.VARCHAR_ARRAY, true);
  private static final TableFunction.ParameterDefn PREFIXES_PARAM_DEFN = new BaseTableFunction.Parameter(PREFIXES_PARAMETER, TableFunction.ParameterType.VARCHAR_ARRAY, true);
  private static final TableFunction.ParameterDefn OBJECTS_PARAM_DEFN = new BaseTableFunction.Parameter(OBJECTS_PARAMETER, TableFunction.ParameterType.VARCHAR_ARRAY, true);
  private static final TableFunction.ParameterDefn PATTERN_PARAM_DEFN = new BaseTableFunction.Parameter(PATTERN_PARAMETER, TableFunction.ParameterType.VARCHAR, true);


  static class PolarisS3ConnectionFunctionSpec implements PolarisTableFunctionSpec
  {
    public static final String FUNCTION_NAME = "POLARIS_S3_CONNECTION";
    private @NotNull final String connectionName;
    private @Nullable final List<String> uris;
    private @Nullable final List<String> prefixes;
    private @Nullable final List<String> objects;
    private @Nullable final String pattern;

    @JsonCreator
    public PolarisS3ConnectionFunctionSpec(
        @JsonProperty("connectionName") @NotNull String connectionName,
        @JsonProperty("uris") @Nullable List<String> uris,
        @JsonProperty("prefixes") @Nullable List<String> prefixes,
        @JsonProperty("objects") @Nullable List<String> objects,
        @JsonProperty("pattern") @Nullable String pattern)
    {
      this.connectionName = connectionName;
      this.uris = uris;
      this.prefixes = prefixes;
      this.objects = objects;
      this.pattern = pattern;
    }

    @NotNull
    @JsonProperty("connectionName")
    public String getConnectionName()
    {
      return connectionName;
    }

    @Nullable
    @JsonProperty("uris")
    public List<String> getUris()
    {
      return uris;
    }

    @Nullable
    @JsonProperty("prefixes")
    public List<String> getPrefixes()
    {
      return prefixes;
    }

    @Nullable
    @JsonProperty("objects")
    public List<String> getObjects()
    {
      return objects;
    }

    @Nullable
    @JsonProperty("pattern")
    public String getPattern()
    {
      return pattern;
    }


    @Override
    public String toString()
    {
      return "PolarisS3ConnectionFunctionSpec{" +
             "name='" + FUNCTION_NAME + '\'' +
             ", connectionName=" + connectionName +
             ", uris=" + uris +
             ", prefixes=" + prefixes +
             ", objects=" + objects +
             ", pattern=" + pattern +
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
      PolarisS3ConnectionFunctionSpec that = (PolarisS3ConnectionFunctionSpec) o;
      return Objects.equals(connectionName, that.connectionName)
             && Objects.equals(uris, that.uris)
             && Objects.equals(prefixes, that.prefixes)
             && Objects.equals(objects, that.objects)
             && Objects.equals(pattern, that.pattern);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(connectionName, uris, prefixes, objects, pattern);
    }
  }

  public PolarisS3ConnectionInputSourceDefn(
      final TableDefnRegistry registry,
      final PolarisTableFunctionResolver resolver)
  {
    super(resolver);
    bind(registry);
  }

  @Nullable
  @Override
  protected Class<? extends InputSource> inputSourceClass()
  {
    // input source is really S3InputSource which is in another extension.
    // OK to return null here, as this function is not used for Polaris resolved
    // input source.
    return null;
  }

  @Override
  protected PolarisTableFunctionSpec convertArgsToTblFnDefn(Map<String, Object> args)
  {
    final String connectionName = CatalogUtils.getString(args, CONNECTION_NAME_PARAMETER);
    final List<String> uris = CatalogUtils.getStringArray(args, URIS_PARAMETER);
    final List<String> prefixes = CatalogUtils.getStringArray(args, PREFIXES_PARAMETER);
    final List<String> objects = CatalogUtils.getStringArray(args, OBJECTS_PARAMETER);
    final String pattern = CatalogUtils.getString(args, PATTERN_PARAMETER);

    // Sanity check args before constructing the spec.
    if (connectionName == null) {
      throw InvalidInput.exception("Must provide a value for the [%s] parameter.", CONNECTION_NAME_PARAMETER);
    }

    final int hasUris = !CollectionUtils.isNullOrEmpty(uris) ? 1 : 0;
    final int hasPrefixes = !CollectionUtils.isNullOrEmpty(prefixes) ? 1 : 0;
    final int hasObjects = !CollectionUtils.isNullOrEmpty(objects) ? 1 : 0;
    final int hasPattern = !Strings.isNullOrEmpty(pattern) ? 1 : 0;
    final int requestParam = hasObjects + hasPrefixes + hasUris + hasPattern;
    if (requestParam == 0) {
      throw InvalidInput.exception("Must provide a non-empty value for one of [%s, %s, %s, %s] parameters.",
                                   URIS_PARAMETER, PREFIXES_PARAMETER, OBJECTS_PARAMETER, PATTERN_PARAMETER);
    }

    if (requestParam > 1) {
      throw InvalidInput.exception("Exactly one of [%s, %s, %s, %s] must be specified.",
                                   URIS_PARAMETER, PREFIXES_PARAMETER, OBJECTS_PARAMETER, PATTERN_PARAMETER);
    }
    return new PolarisS3ConnectionFunctionSpec(connectionName, uris, prefixes, objects, pattern);
  }

  @Override
  protected List<TableFunction.ParameterDefn> adHocTableFnParameters()
  {
    return Arrays.asList(
        CONNECTION_NAME_PARAM_DEFN,
        URI_PARAM_DEFN,
        PREFIXES_PARAM_DEFN,
        OBJECTS_PARAM_DEFN,
        PATTERN_PARAM_DEFN
    );
  }
}
