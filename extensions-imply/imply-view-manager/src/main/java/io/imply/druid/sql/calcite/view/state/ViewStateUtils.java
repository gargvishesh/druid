/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.sql.calcite.view.ImplyViewDefinition;
import org.apache.druid.java.util.common.ISE;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ViewStateUtils
{
  public static final TypeReference<Map<String, ImplyViewDefinition>> VIEW_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, ImplyViewDefinition>>()
      {
      };

  public static byte[] serializeViewMap(
      ObjectMapper objectMapper,
      Map<String, ImplyViewDefinition> viewMap
  )
  {
    try {
      return objectMapper.writeValueAsBytes(viewMap);
    }
    catch (IOException ioe) {
      throw new ISE(ioe, "Couldn't serialize view map!");
    }
  }

  public static Map<String, ImplyViewDefinition> deserializeViewMap(
      ObjectMapper objectMapper,
      byte[] viewMapBytes
  )
  {
    Map<String, ImplyViewDefinition> viewMap;
    if (viewMapBytes == null) {
      viewMap = new HashMap<>();
    } else {
      try {
        viewMap = objectMapper.readValue(viewMapBytes, VIEW_MAP_TYPE_REFERENCE);
      }
      catch (IOException ioe) {
        throw new RuntimeException("Couldn't deserialize viewMap!", ioe);
      }
    }
    return viewMap;
  }
}
