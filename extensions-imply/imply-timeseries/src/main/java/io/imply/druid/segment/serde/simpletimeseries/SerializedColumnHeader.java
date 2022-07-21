/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.base.Objects;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class SerializedColumnHeader
{
  // header size is 4 bytes for word alignment for LZ4 (minmatch) compression
  private static final int HEADER_SIZE_BYTES = 4;
  private static final int USE_INTEGER_MASK = 0x80;
  private static final int VERSION_INDEX = 0;
  private static final int ENCODING_INDEX = 1;

  private final LongSerializer longSerializer = new LongSerializer();
  private final byte[] bytes;
  private final long minTimestamp;

  private SerializedColumnHeader(byte[] bytes, long minTimestamp)
  {
    this.bytes = bytes;
    this.minTimestamp = minTimestamp;
  }

  public SerializedColumnHeader(byte version, boolean useIntegerDeltas, long minTimestamp)
  {
    this.minTimestamp = minTimestamp;
    bytes = new byte[HEADER_SIZE_BYTES];
    bytes[VERSION_INDEX] = version;

    if (useIntegerDeltas) {
      bytes[ENCODING_INDEX] |= USE_INTEGER_MASK;
    }
  }

  public static SerializedColumnHeader fromBuffer(ByteBuffer byteBuffer)
  {
    byte[] bytes = new byte[HEADER_SIZE_BYTES];

    byteBuffer.get(bytes);

    long minTimestamp = byteBuffer.getLong();

    return new SerializedColumnHeader(bytes, minTimestamp);
  }

  public SimpleTimeSeriesSerde createSimpleTimeSeriesSerde()
  {
    return SimpleTimeSeriesSerde.create(minTimestamp, isUseIntegerDeltas());
  }

  public void transferTo(WritableByteChannel channel) throws IOException
  {
    channel.write(ByteBuffer.wrap(bytes));
    channel.write(longSerializer.serialize(minTimestamp));
  }

  public byte getVersion()
  {
    return bytes[VERSION_INDEX];
  }

  public boolean isUseIntegerDeltas()
  {
    return (bytes[ENCODING_INDEX] & USE_INTEGER_MASK) != 0;
  }

  public long getMinTimestamp()
  {
    return minTimestamp;
  }

  public int getSerializedSize()
  {
    return HEADER_SIZE_BYTES + Long.BYTES;
  }


  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                      .add("longSerializer", longSerializer)
                      .add("bytes", bytes)
                      .add("minTimestamp", minTimestamp)
                      .toString();
  }
}
