/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import com.google.common.io.ByteStreams;
import io.imply.druid.talaria.frame.ArenaMemoryAllocator;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.testutil.FrameSequenceBuilder;
import io.imply.druid.talaria.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;

public class ReadableInputStreamFrameChannelTest extends InitializedNullHandlingTest
{

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  final IncrementalIndexStorageAdapter adapter =
      new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

  ExecutorService executorService = Execs.singleThreaded("input-stream-fetcher-test");

  @Test
  public void readSimpleFrameFile()
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        inputStream,
        "readSimpleFrameFile",
        executorService
    );

    readableInputStreamFrameChannel.startReading();
    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(adapter.getRowSignature())
        )
    );
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.doneReading();

  }


  @Test
  public void readEmptyFrameFile() throws IOException
  {
    final File file = FrameTestUtil.writeFrameFile(Sequences.empty(), temporaryFolder.newFile());
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        Files.newInputStream(file.toPath()),
        "readEmptyFrameFile",
        executorService
    );

    readableInputStreamFrameChannel.startReading();

    Assert.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
        readableInputStreamFrameChannel,
        FrameReader.create(adapter.getRowSignature())
    ).toList().size(), 0);
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.doneReading();
  }

  @Test
  public void testZeroBytesFrameFile() throws IOException
  {
    final File file = temporaryFolder.newFile();
    FileOutputStream outputStream = new FileOutputStream(file);
    outputStream.write(new byte[0]);
    outputStream.close();

    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        Files.newInputStream(file.toPath()),
        "testZeroBytesFrameFile",
        executorService
    );

    readableInputStreamFrameChannel.startReading();

    Assert.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
        readableInputStreamFrameChannel,
        FrameReader.create(adapter.getRowSignature())
    ).toList().size(), 0);
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.doneReading();

  }

  @Test
  public void readTruncatedFrameFile() throws IOException
  {
    final int allocatorSize = 64000;
    final int truncatedSize = 30000; // Holds two full frames + one partial frame, after compression.

    final IncrementalIndexStorageAdapter adapter =
        new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

    final File file = FrameTestUtil.writeFrameFile(
        FrameSequenceBuilder.fromAdapter(adapter)
                            .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(allocatorSize)))
                            .frameType(FrameType.ROW_BASED)
                            .frames(),
        temporaryFolder.newFile()
    );

    final byte[] truncatedFile = new byte[truncatedSize];

    try (final FileInputStream in = new FileInputStream(file)) {
      ByteStreams.readFully(in, truncatedFile);
    }


    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        new ByteArrayInputStream(truncatedFile),
        "readTruncatedFrameFile",
        executorService
    );

    readableInputStreamFrameChannel.startReading();

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Incomplete or missing frame at end of stream");

    Assert.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
        readableInputStreamFrameChannel,
        FrameReader.create(adapter.getRowSignature())
    ).toList().size(), 0);
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.doneReading();
  }

  @Test
  public void readIncorrectFrameFile() throws IOException
  {
    final File file = temporaryFolder.newFile();
    FileOutputStream outputStream = new FileOutputStream(file);
    outputStream.write(10);
    outputStream.close();

    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        Files.newInputStream(file.toPath()),
        "readIncorrectFrameFile",
        executorService
    );

    readableInputStreamFrameChannel.startReading();

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Incomplete or missing frame at end of stream");

    Assert.assertEquals(FrameTestUtil.readRowsFromFrameChannel(
        readableInputStreamFrameChannel,
        FrameReader.create(adapter.getRowSignature())
    ).toList().size(), 0);
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.doneReading();

  }


  @Test
  public void closeInputStreamWhileReading() throws IOException
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        inputStream,
        "closeInputStreamWhileReading",
        executorService
    );
    inputStream.close();
    readableInputStreamFrameChannel.startReading();

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Found error while reading input stream");
    FrameTestUtil.assertRowsEqual(
        FrameTestUtil.readRowsFromAdapter(adapter, null, false),
        FrameTestUtil.readRowsFromFrameChannel(
            readableInputStreamFrameChannel,
            FrameReader.create(adapter.getRowSignature())
        )
    );
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.doneReading();
  }


  @Test
  public void closeInputStreamWhileReadingCheckError() throws IOException, InterruptedException
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        inputStream,
        "closeInputStreamWhileReadingCheckError",
        executorService
    );

    inputStream.close();
    readableInputStreamFrameChannel.startReading();

    expectedException.expect(ISE.class);
    expectedException.expectMessage("Found error while reading input stream");

    while (!readableInputStreamFrameChannel.canRead()) {
      Thread.sleep(10);
    }
    readableInputStreamFrameChannel.read().getOrThrow();
    Assert.assertTrue(readableInputStreamFrameChannel.isFinished());
    readableInputStreamFrameChannel.doneReading();
  }

  @Test
  public void testInvalidStateCanRead()
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        inputStream,
        "testInvalidStateCanRead",
        executorService
    );
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Please call startReading method");
    readableInputStreamFrameChannel.canRead();
    readableInputStreamFrameChannel.doneReading();
  }


  @Test
  public void testInvalidStateRead()
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        inputStream,
        "testInvalidStateRead",
        executorService
    );
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Please call startReading method");
    readableInputStreamFrameChannel.read();
    readableInputStreamFrameChannel.doneReading();
  }


  @Test
  public void testInvalidStateReadabilityFuture()
  {
    InputStream inputStream = getInputStream();
    ReadableInputStreamFrameChannel readableInputStreamFrameChannel = new ReadableInputStreamFrameChannel(
        inputStream,
        "testInvalidStateReadabilityFuture",
        executorService
    );
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Please call startReading method");
    readableInputStreamFrameChannel.readabilityFuture();
    readableInputStreamFrameChannel.doneReading();
  }

  private InputStream getInputStream()
  {
    try {
      final File file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter).maxRowsPerFrame(10).frameType(FrameType.ROW_BASED).frames(),
          temporaryFolder.newFile()
      );
      return new FileInputStream(file);
    }
    catch (IOException e) {
      throw new ISE(e, "Unable to create file input stream");
    }
  }

}
