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
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.data.ColumnCapacityExceededException;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.serde.MetaSerdeHelper;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class CompressedVariableSizedBlobColumnSerializer implements Serializer
{
  private static final MetaSerdeHelper<CompressedVariableSizedBlobColumnSerializer> META_SERDE_HELPER = MetaSerdeHelper
      .firstWriteByte((CompressedVariableSizedBlobColumnSerializer x) -> (byte) 0x01)
      .writeInt(x -> x.numValues);

  private final String filenameBase;
  private final String offsetsFile;
  private final String blobsFile;
  private final SegmentWriteOutMedium segmentWriteOutMedium;
  private final CompressionStrategy compression;

  private int numValues;
  private long currentOffset;

  private CompressedLongsSerializer offsetsSerializer;
  private CompressedBlockSerializer valuesSerializer;

  CompressedVariableSizedBlobColumnSerializer(
      final String filenameBase,
      final SegmentWriteOutMedium segmentWriteOutMedium,
      final CompressionStrategy compression
  )
  {
    this.filenameBase = filenameBase;
    this.offsetsFile = getCompressedOffsetsFileName(filenameBase);
    this.blobsFile = getCompressedBlobsFileName(filenameBase);
    this.segmentWriteOutMedium = segmentWriteOutMedium;
    this.compression = compression;
    this.numValues = 0;
  }

  public void open() throws IOException
  {
    numValues = 0;
    currentOffset = 0;
    offsetsSerializer = new CompressedLongsSerializer(segmentWriteOutMedium, compression);
    offsetsSerializer.open();

    valuesSerializer = new CompressedBlockSerializer(
        segmentWriteOutMedium,
        compression,
        CompressedPools.BUFFER_SIZE
    );
    valuesSerializer.open();
  }

  public void addValue(byte[] bytes) throws IOException
  {
    valuesSerializer.addValue(bytes);

    currentOffset += bytes.length;
    offsetsSerializer.add(currentOffset);
    numValues++;
    if (numValues < 0) {
      throw new ColumnCapacityExceededException(filenameBase);
    }
  }

  public void addValue(ByteBuffer bytes) throws IOException
  {
    currentOffset += bytes.remaining();
    valuesSerializer.addValue(bytes);
    offsetsSerializer.add(currentOffset);
    numValues++;
    if (numValues < 0) {
      throw new ColumnCapacityExceededException(filenameBase);
    }
  }

  @Override
  public long getSerializedSize()
  {
    // offsets and blobs stored in their own files, so our only size is metadata
    return META_SERDE_HELPER.size(this);
  }

  @Override
  public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
  {
    META_SERDE_HELPER.writeTo(channel, this);
    try (SmooshedWriter sub = smoosher.addWithSmooshedWriter(offsetsFile, offsetsSerializer.getSerializedSize())) {
      offsetsSerializer.writeTo(sub, smoosher);
    }
    try (SmooshedWriter sub = smoosher.addWithSmooshedWriter(blobsFile, valuesSerializer.getSerializedSize())) {
      valuesSerializer.writeTo(sub, smoosher);
    }
  }

  public static String getCompressedOffsetsFileName(String filenameBase)
  {
    return filenameBase + "_offsets";
  }

  public static String getCompressedBlobsFileName(String filenameBase)
  {
    return filenameBase + "_compressed";
  }
}
