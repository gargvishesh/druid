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
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.BaseTableFunction;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PolarisUploadedInputSourceDefn extends BasePolarisInputSourceDefn
{
  public static final String FILES_PARAMETER = "files";

  private static final TableFunction.ParameterDefn FILES_PARAM_DEFN =
      new BaseTableFunction.Parameter(FILES_PARAMETER, TableFunction.ParameterType.VARCHAR_ARRAY, false);

  static class PolarisUploadedFunctionSpec implements PolarisTableFunctionSpec
  {
    public static final String FUNCTION_NAME = "POLARIS_UPLOADED";
    private @NotNull final List<String> files;

    @JsonCreator
    public PolarisUploadedFunctionSpec(
        @JsonProperty("files") @NotNull List<String> files)
    {
      this.files = files;
    }

    @Nonnull
    @JsonProperty("files")
    public List<String> getFiles()
    {
      return files;
    }

    @Override
    public String toString()
    {
      return "PolarisUploadedFunctionSpec{" +
             "name='" + FUNCTION_NAME + '\'' +
             ", files=" + files +
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
      PolarisUploadedFunctionSpec that = (PolarisUploadedFunctionSpec) o;
      return Objects.equals(files, that.files);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(files);
    }
  }

  public PolarisUploadedInputSourceDefn(
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
    // input source  is really S3InputSource which is in another extension.
    // OK to return null here, as this function is not used for Polaris resolved
    // input source.
    return null;
  }

  @Override
  protected PolarisTableFunctionSpec convertArgsToTblFnDefn(Map<String, Object> args)
  {
    final List<String> files = CatalogUtils.getStringArray(args, FILES_PARAMETER);
    if (CollectionUtils.isNullOrEmpty(files)) {
      throw new IAE("Must provide a non-empty value for the [%s] parameter", FILES_PARAMETER);
    }
    return new PolarisUploadedFunctionSpec(files);
  }

  @Override
  protected List<TableFunction.ParameterDefn> adHocTableFnParameters()
  {
    return Collections.singletonList(FILES_PARAM_DEFN);
  }
}
