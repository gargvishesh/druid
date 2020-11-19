/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.files;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputSource;

public abstract class BaseFileStore implements FileStore
{
  private final ObjectMapper jsonMapper;

  public BaseFileStore(ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public InputSource makeInputSource(String file)
  {
    try {
      return jsonMapper.readValue(jsonMapper.writeValueAsString(makeInputSourceMap(file)), InputSource.class);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
