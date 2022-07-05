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
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.java.util.common.io.smoosh.SmooshedWriter;
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class CompressedVariableSizeBlobColumnTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testSomeValues() throws IOException
  {
    // value sizes increase until they span at least 3 pages of compressed buffers
    final File tmpFile = tempFolder.newFolder();
    final FileSmoosher smoosher = new FileSmoosher(tmpFile);

    final File tmpFile2 = tempFolder.newFolder();
    final SegmentWriteOutMedium writeOutMedium =
        TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(tmpFile2);

    final String fileNameBase = "test";

    final CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;
    CompressedVariableSizedBlobColumnSerializer serializer = new CompressedVariableSizedBlobColumnSerializer(
        fileNameBase,
        writeOutMedium,
        compressionStrategy
    );
    serializer.open();

    int numWritten = 0;
    final Random r = ThreadLocalRandom.current();
    final List<byte[]> values = new ArrayList<>();
    for (int i = 0, offset = 0; offset < CompressedPools.BUFFER_SIZE * 4; i++, offset = 1 << i) {
      byte[] value = new byte[offset];
      r.nextBytes(value);
      values.add(value);
      serializer.addValue(value);
      numWritten++;
    }

    SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize());
    serializer.writeTo(writer, smoosher);
    writer.close();
    smoosher.close();
    SmooshedFileMapper fileMapper = SmooshedFileMapper.load(tmpFile);

    ByteBuffer base = fileMapper.mapFile(fileNameBase);

    CompressedVariableSizedBlobColumn column = CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
        fileNameBase,
        base,
        ByteOrder.nativeOrder(),
        fileMapper
    ).get();
    for (int row = 0; row < numWritten; row++) {
      ByteBuffer value = column.get(row);
      byte[] bytes = new byte[value.limit()];
      value.get(bytes);
      Assert.assertArrayEquals("Row " + row, values.get(row), bytes);
    }
    column.close();
    fileMapper.close();
  }

  @Test
  public void testSomeValuesByteBuffers() throws IOException
  {
    // value sizes increase until they span at least 3 pages of compressed buffers
    final File tmpFile = tempFolder.newFolder();
    final FileSmoosher smoosher = new FileSmoosher(tmpFile);

    final File tmpFile2 = tempFolder.newFolder();
    final SegmentWriteOutMedium writeOutMedium =
        TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(tmpFile2);

    final String fileNameBase = "test";

    final CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;
    CompressedVariableSizedBlobColumnSerializer serializer = new CompressedVariableSizedBlobColumnSerializer(
        fileNameBase,
        writeOutMedium,
        compressionStrategy
    );
    serializer.open();

    int numWritten = 0;
    final Random r = ThreadLocalRandom.current();
    final List<byte[]> values = new ArrayList<>();
    for (int i = 0, offset = 0; offset < CompressedPools.BUFFER_SIZE * 4; i++, offset = 1 << i) {
      byte[] value = new byte[offset];
      r.nextBytes(value);
      values.add(value);
      serializer.addValue(ByteBuffer.wrap(value));
      numWritten++;
    }

    SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize());
    serializer.writeTo(writer, smoosher);
    writer.close();
    smoosher.close();
    SmooshedFileMapper fileMapper = SmooshedFileMapper.load(tmpFile);

    ByteBuffer base = fileMapper.mapFile(fileNameBase);

    CompressedVariableSizedBlobColumn column = CompressedVariableSizedBlobColumnSupplier.fromByteBuffer(
        fileNameBase,
        base,
        ByteOrder.nativeOrder(),
        fileMapper
    ).get();
    for (int row = 0; row < numWritten; row++) {
      ByteBuffer value = column.get(row);
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      Assert.assertArrayEquals("Row " + row, values.get(row), bytes);
    }
    column.close();
    fileMapper.close();
  }

  @Test
  public void testLongs() throws IOException
  {
    // value sizes increase until they span at least 3 pages of compressed buffers
    final File tmpFile = tempFolder.newFolder();
    final FileSmoosher smoosher = new FileSmoosher(tmpFile);

    final File tmpFile2 = tempFolder.newFolder();
    final SegmentWriteOutMedium writeOutMedium =
        TmpFileSegmentWriteOutMediumFactory.instance().makeSegmentWriteOutMedium(tmpFile2);

    final String fileNameBase = "test";

    final CompressionStrategy compressionStrategy = CompressionStrategy.LZ4;
    CompressedLongsSerializer serializer = new CompressedLongsSerializer(
        writeOutMedium,
        compressionStrategy
    );
    serializer.open();

    final Random r = ThreadLocalRandom.current();
    int numWritten = 0;
    final List<Long> values = new ArrayList<>();
    for (int i = 0; i < 1_000_000; i++) {
      long l = r.nextLong();
      values.add(l);
      serializer.add(l);
      numWritten++;
    }

    SmooshedWriter writer = smoosher.addWithSmooshedWriter(fileNameBase, serializer.getSerializedSize());
    serializer.writeTo(writer, smoosher);
    writer.close();
    smoosher.close();
    SmooshedFileMapper fileMapper = SmooshedFileMapper.load(tmpFile);

    ByteBuffer base = fileMapper.mapFile(fileNameBase);

    try (CompressedLongsReader reader = CompressedLongsReader.fromByteBuffer(base, ByteOrder.nativeOrder()).get()) {
      for (int row = 0; row < numWritten; row++) {
        for (int i = 0; i < 5; i++) {
          long l = reader.get(row);
          Assert.assertEquals("Row " + row, values.get(row).longValue(), l);
        }
      }
    }
  }
}
