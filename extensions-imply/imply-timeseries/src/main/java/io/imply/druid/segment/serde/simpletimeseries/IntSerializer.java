/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IntSerializer implements NumberSerializer
{
  private final ByteBuffer intValueByteBuffer =
      ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.nativeOrder());

  @Override
  public ByteBuffer serialize(long value)
  {
    intValueByteBuffer.clear();
    intValueByteBuffer.putInt(Ints.checkedCast(value)).flip();

    return intValueByteBuffer;
  }

  @Override
  public int getSerializedSize()
  {
    return Integer.BYTES;
  }
}
