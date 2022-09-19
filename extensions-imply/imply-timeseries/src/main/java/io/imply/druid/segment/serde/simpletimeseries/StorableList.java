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
 * contains a list that is prepared to be stored in a buffer as well as the size and a flag indicating if it's
 * RLE-encoded or not
 */
public interface StorableList
{
  StorableList EMPTY = new StorableList()
  {
    @Override
    public boolean isRle()
    {
      return false;
    }

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

  boolean isRle();

  void store(ByteBuffer byteBuffer);

  int getSerializedSize();
}
