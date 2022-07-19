/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.polaris;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.List;

/**
 * A macro for specifying the signature of the table along with its declaration. Only meant to be used for Imply
 * internal usecases.
 */
public class PolarisExplainTableMacro implements TableMacro
{
  private final ObjectMapper jsonMapper;

  @Inject
  public PolarisExplainTableMacro(@Json final ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public TranslatableTable apply(final List<Object> arguments)
  {
    try {
      RowSignature signature = jsonMapper.readValue((String) arguments.get(0), RowSignature.class);

      return new DruidTable(
          InlineDataSource.fromIterable(ImmutableList.of(), signature),
          signature,
          jsonMapper,
          false,
          false
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<FunctionParameter> getParameters()
  {
    return ImmutableList.of(
        new FunctionParameter()
        {
          @Override
          public int getOrdinal()
          {
            return 0;
          }

          @Override
          public String getName()
          {
            return "signature";
          }

          @Override
          public RelDataType getType(RelDataTypeFactory typeFactory)
          {
            return typeFactory.createJavaType(String.class);
          }

          @Override
          public boolean isOptional()
          {
            return false;
          }
        }
    );
  }
}
