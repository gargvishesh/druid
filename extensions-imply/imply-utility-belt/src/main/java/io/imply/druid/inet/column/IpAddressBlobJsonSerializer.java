/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

import java.io.IOException;

public class IpAddressBlobJsonSerializer extends JsonSerializer<IpAddressBlob>
{
  @Override
  public void serialize(
      IpAddressBlob ipAddressBlob,
      JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider
  ) throws IOException
  {
    jsonGenerator.writeBinary(ipAddressBlob.getBytes());
  }
}
