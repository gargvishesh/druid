/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.Supplier;

public class CompressedVariableSizedBlobColumnSupplier implements Supplier<CompressedVariableSizedBlobColumn>
{
  public static final byte VERSION = 0x01;

  public static CompressedVariableSizedBlobColumnSupplier fromByteBuffer(
      String filenameBase,
      ByteBuffer buffer,
      ByteOrder order,
      SmooshedFileMapper mapper
  ) throws IOException
  {

    byte versionFromBuffer = buffer.get();
    if (versionFromBuffer == VERSION) {
      final int numElements = buffer.getInt();
      // offsets and blobs are stored in their own files
      final ByteBuffer offsetsBuffer = mapper.mapFile(
          CompressedVariableSizedBlobColumnSerializer.getCompressedOffsetsFileName(filenameBase)
      );
      final ByteBuffer dataBuffer = mapper.mapFile(
          CompressedVariableSizedBlobColumnSerializer.getCompressedBlobsFileName(filenameBase)
      );
      return new CompressedVariableSizedBlobColumnSupplier(offsetsBuffer, dataBuffer, order, numElements);
    }
    throw new IAE("Unknown version[%s]", versionFromBuffer);
  }

  private final int numElements;

  private final Supplier<CompressedLongsReader> offsetReaderSupplier;
  private final Supplier<CompressedBlockReader> blockDataReaderSupplier;
  final ByteOrder order;

  public CompressedVariableSizedBlobColumnSupplier(
      ByteBuffer offsetsBuffer,
      ByteBuffer dataBuffer,
      ByteOrder order,
      int numElements
  )
  {
    this.order = order;
    this.numElements = numElements;
    this.offsetReaderSupplier = CompressedLongsReader.fromByteBuffer(offsetsBuffer, order);
    this.blockDataReaderSupplier = CompressedBlockReader.fromByteBuffer(dataBuffer, order);
  }

  @Override
  public CompressedVariableSizedBlobColumn get()
  {
    return new CompressedVariableSizedBlobColumn(
        numElements,
        offsetReaderSupplier.get(),
        blockDataReaderSupplier.get()
    );
  }
}
