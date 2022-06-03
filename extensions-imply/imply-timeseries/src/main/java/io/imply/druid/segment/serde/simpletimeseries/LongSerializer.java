/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class LongSerializer implements NumberSerializer
{
  private final ByteBuffer longValueByteBuffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder());

  @Override
  public ByteBuffer serialize(long value)
  {
    longValueByteBuffer.clear();
    longValueByteBuffer.putLong(value).flip();

    return longValueByteBuffer;
  }

  @Override
  public int getSerializedSize()
  {
    return Long.BYTES;
  }
}
