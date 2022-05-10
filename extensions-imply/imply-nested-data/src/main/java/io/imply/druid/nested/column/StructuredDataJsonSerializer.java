/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.druid.java.util.common.jackson.JacksonUtils;

import java.io.IOException;

public class StructuredDataJsonSerializer extends JsonSerializer<StructuredData>
{
  @Override
  public void serialize(
      StructuredData structuredData,
      JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider
  ) throws IOException
  {
    JacksonUtils.writeObjectUsingSerializerProvider(jsonGenerator, serializerProvider, structuredData.getValue());
  }
}
