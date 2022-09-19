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

/**
 * useful when work needs to be done to prepare for serialization (eg encoding) which is also necessary
 * to know how large a buffer is needed. Hence, returns both the size and a method to store in a buffer
 * caller must allocate of sufficient size
 */
public interface StorableBuffer
{
  StorableBuffer EMPTY = new StorableBuffer()
  {
    @Override
    public void store(ByteBuffer byteBuffer)
    {
    }

    @Override
    public int getSerializedSize()
    {
      return 0;
    }
  };

  void store(ByteBuffer byteBuffer);

  int getSerializedSize();
}
