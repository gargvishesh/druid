/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.RE;

import javax.annotation.Nullable;

public class JsonColumnIndexer extends NestedDataColumnIndexer
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Nullable
  @Override
  public StructuredData processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
  {
    if (dimValues == null) {
      hasNulls = true;
      return null;
    }
    if (dimValues instanceof String) {
      final String stringValue = (String) dimValues;
      if (stringValue.startsWith("[") || stringValue.startsWith("{")) {
        try {
          Object deserialized = JSON_MAPPER.readValue(stringValue, Object.class);
          indexerProcessor.processFields(deserialized);
          return new StructuredData(deserialized);
        }
        catch (JsonProcessingException e) {
          throw new RE(e, "Failed to deserialize [%s] as JSON", stringValue);
        }
      }
    }
    return super.processRowValsToUnsortedEncodedKeyComponent(dimValues, reportParseExceptions);
  }
}
