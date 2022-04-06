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
import org.apache.druid.segment.EncodedKeyComponent;

import javax.annotation.Nullable;

public class JsonColumnIndexer extends NestedDataColumnIndexer
{
  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  @Override
  public EncodedKeyComponent<StructuredData> processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
  {
    if (dimValues instanceof String) {
      final String stringValue = (String) dimValues;
      if (stringValue.startsWith("[")
          || stringValue.startsWith("{")
          || stringValue.startsWith("\"")
          || Character.isDigit(stringValue.charAt(0))) {
        try {
          final Object deserialized = JSON_MAPPER.readValue(stringValue, Object.class);
          return super.processRowValsToUnsortedEncodedKeyComponent(deserialized, reportParseExceptions);
        }
        catch (JsonProcessingException e) {
          throw new RE(e, "Failed to deserialize [%s] as JSON", stringValue);
        }
      }
    }
    return super.processRowValsToUnsortedEncodedKeyComponent(dimValues, reportParseExceptions);
  }
}
