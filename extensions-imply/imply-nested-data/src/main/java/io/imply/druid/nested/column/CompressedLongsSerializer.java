/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class CompressedLongsSerializer implements Serializer
{
  private final CompressedBlockSerializer blockSerializer;
  private final ByteBuffer longValueConverter = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder());

  public CompressedLongsSerializer(SegmentWriteOutMedium segmentWriteOutMedium, CompressionStrategy compression)
  {
    this.blockSerializer = new CompressedBlockSerializer(
        segmentWriteOutMedium,
        compression,
        CompressedPools.BUFFER_SIZE
    );
  }

  public void open() throws IOException
  {
    blockSerializer.open();
  }

  public void add(long value) throws IOException
  {
    longValueConverter.clear();
    longValueConverter.putLong(value);
    longValueConverter.rewind();
    blockSerializer.addValue(longValueConverter);
  }

  @Override
  public long getSerializedSize() throws IOException
  {
    return blockSerializer.getSerializedSize();
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    blockSerializer.writeTo(channel, smoosher);
  }
}
