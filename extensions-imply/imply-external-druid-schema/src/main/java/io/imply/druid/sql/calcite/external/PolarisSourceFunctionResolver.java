/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.sql.calcite.schema.tables.mapping.ExternalTableFunctionMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;

public class PolarisSourceFunctionResolver implements PolarisTableFunctionResolver
{
  private ExternalTableFunctionMapper tableFunctionMapper;
  private ObjectMapper jsonMapper;

  public PolarisSourceFunctionResolver(ExternalTableFunctionMapper m, ObjectMapper mapper)
  {
    this.tableFunctionMapper = m;
    this.jsonMapper = mapper;
  }

  @Override
  public PolarisExternalTableSpec resolve(PolarisTableFunctionSpec fn)
  {
    if (null == fn) {
      throw new IAE("Table function spec cannot be null.");
    }
    try {
      byte[] externalTableSpecBytes = tableFunctionMapper.getTableFunctionMapping(jsonMapper.writeValueAsBytes(fn));
      return jsonMapper.readValue(externalTableSpecBytes, PolarisExternalTableSpec.class);
    }
    catch (JsonProcessingException e) {
      throw new IAE(StringUtils.format("Table function spec is malformed [%s]", e));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
