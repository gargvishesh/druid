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
import java.util.Arrays;

public class RowHeader
{
  // header size is 4 bytes for word alignment for LZ4 (minmatch) compression
  private static final int HEADER_SIZE_BYTES = 4;
  private static final int TIMESTAMPS_RLE_MASK = 0x01;
  private static final int VALUES_RLE_MASK = 0x02;
  private static final int VERSION_INDEX = 0;
  private static final int ENCODING_INDEX = 1;

  public static int getSerializedSize()
  {
    return HEADER_SIZE_BYTES;
  }

  public static class Reader
  {
    private byte[] bytes = new byte[HEADER_SIZE_BYTES];

    public Reader readBytes(ByteBuffer byteBuffer)
    {
      byteBuffer.get(bytes);

      return this;
    }

    public byte getVersion()
    {
      return bytes[VERSION_INDEX];
    }

    public boolean isTimestampsRle()
    {
      return (bytes[ENCODING_INDEX] & TIMESTAMPS_RLE_MASK) != 0;
    }

    public boolean isValuesRle()
    {
      return (bytes[ENCODING_INDEX] & VALUES_RLE_MASK) != 0;
    }
  }

  public static class Writer
  {
    private byte[] bytes = new byte[HEADER_SIZE_BYTES];

    public Writer reset()
    {
      Arrays.fill(bytes, (byte) 0);

      return this;
    }

    public Writer setBytes(byte[] bytes)
    {
      this.bytes = bytes;

      return this;
    }

    public Writer setVersion(byte version)
    {
      bytes[VERSION_INDEX] = version;
      return this;
    }

    public Writer setTimestampsRle(boolean timestampsRle)
    {
      if (timestampsRle) {
        bytes[ENCODING_INDEX] |= TIMESTAMPS_RLE_MASK;
      } else {
        bytes[ENCODING_INDEX] &= ~TIMESTAMPS_RLE_MASK;
      }

      return this;
    }

    public Writer setValuesRle(boolean valuesRle)
    {
      if (valuesRle) {
        bytes[ENCODING_INDEX] |= VALUES_RLE_MASK;
      } else {
        bytes[ENCODING_INDEX] &= ~VALUES_RLE_MASK;
      }

      return this;
    }

    public void store(ByteBuffer byteBuffer)
    {
      byteBuffer.put(bytes);
    }
  }
}
