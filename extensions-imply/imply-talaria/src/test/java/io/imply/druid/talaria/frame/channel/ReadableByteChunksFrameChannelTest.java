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
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.frame.FrameTestUtil;
import io.imply.druid.talaria.frame.TestFrameSequenceBuilder;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.ArenaMemoryAllocator;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * TODO(gianm): Add tests for bad streams.
 */
@RunWith(Enclosed.class)
public class ReadableByteChunksFrameChannelTest
{
  /**
   * Non-parameterized test cases. Each one is special.
   */
  public static class NonParameterized extends InitializedNullHandlingTest
  {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testZeroBytes()
    {
      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      channel.doneWriting();

      Assert.assertTrue(channel.canRead());

      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage("Incomplete or missing frame at end of stream (id = test, position = 0)");

      channel.read().getOrThrow();
    }

    @Test
    public void testZeroBytesWithSpecialError()
    {
      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      channel.setError(new IllegalArgumentException("test error"));
      channel.doneWriting();

      Assert.assertTrue(channel.canRead());

      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("test error");

      channel.read().getOrThrow();
    }

    @Test
    public void testEmptyFrameFile() throws IOException
    {
      final File file = FrameTestUtil.writeFrameFile(Sequences.empty(), temporaryFolder.newFile());

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      channel.addChunk(Files.toByteArray(file));
      channel.doneWriting();

      while (channel.canRead()) {
        channel.read().getOrThrow();
      }

      Assert.assertTrue(channel.isFinished());
      channel.doneReading();
    }

    @Test
    public void testTruncatedFrameFile() throws IOException
    {
      final int allocatorSize = 64000;
      final int truncatedSize = 30000; // Holds two full frames + one partial frame, after compression.

      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          TestFrameSequenceBuilder.fromAdapter(adapter)
                                  .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(allocatorSize)))
                                  .frames(),
          temporaryFolder.newFile()
      );

      final byte[] truncatedFile = new byte[truncatedSize];

      try (final FileInputStream in = new FileInputStream(file)) {
        ByteStreams.readFully(in, truncatedFile);
      }

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      channel.addChunk(truncatedFile);
      channel.doneWriting();

      Assert.assertTrue(channel.canRead());
      channel.read().getOrThrow(); // Throw away value.

      Assert.assertTrue(channel.canRead());
      channel.read().getOrThrow(); // Throw away value.

      Assert.assertTrue(channel.canRead());

      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage(CoreMatchers.startsWith("Incomplete or missing frame at end of stream"));
      channel.read().getOrThrow();
    }

    @Test
    public void testSetError() throws IOException
    {
      final int allocatorSize = 64000;
      final int errorAtBytePosition = 30000; // Holds two full frames + one partial frame, after compression.

      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          TestFrameSequenceBuilder.fromAdapter(adapter)
                                  .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(allocatorSize)))
                                  .frames(),
          temporaryFolder.newFile()
      );

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      final byte[] fileBytes = Files.toByteArray(file);
      final byte[] chunk1 = new byte[errorAtBytePosition];
      System.arraycopy(fileBytes, 0, chunk1, 0, chunk1.length);
      channel.addChunk(chunk1);
      channel.setError(new ISE("Test error!"));
      channel.doneWriting();

      expectedException.expect(IllegalStateException.class);
      expectedException.expectMessage("Test error!");
      channel.read().getOrThrow();
    }
  }

  /**
   * Parameterized test cases that use various FrameFiles built from {@link TestIndex#getIncrementalTestIndex()}.
   */
  @RunWith(Parameterized.class)
  public static class ParameterizedWithTestIndex extends InitializedNullHandlingTest
  {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final int maxRowsPerFrame;
    private final int chunkSize;

    public ParameterizedWithTestIndex(final int maxRowsPerFrame, final int chunkSize)
    {
      this.maxRowsPerFrame = maxRowsPerFrame;
      this.chunkSize = chunkSize;
    }

    @Parameterized.Parameters(name = "maxRowsPerFrame = {0}, chunkSize = {1}")
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (int maxRowsPerFrame : new int[]{1, 50, Integer.MAX_VALUE}) {
        for (int chunkSize : new int[]{1, 10, 1_000, 5_000, 50_000, 1_000_000}) {
          constructors.add(new Object[]{maxRowsPerFrame, chunkSize});
        }
      }

      return constructors;
    }

    @Test
    public void testWriteFullyThenRead() throws IOException
    {
      // Create a frame file.
      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          TestFrameSequenceBuilder.fromAdapter(adapter).maxRowsPerFrame(maxRowsPerFrame).frames(),
          temporaryFolder.newFile()
      );

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      ListenableFuture<?> backpressureFuture = null;

      Assert.assertEquals(0, channel.getBytesBuffered());

      try (final Chunker chunker = new Chunker(new FileInputStream(file), chunkSize)) {
        byte[] chunk;

        while ((chunk = chunker.nextChunk()) != null) {
          final Optional<ListenableFuture<?>> addVal = channel.addChunk(chunk);

          // Minimally-sized channel means backpressure is exerted as soon as a single frame is available.
          Assert.assertEquals(channel.canRead(), addVal.isPresent());

          if (addVal.isPresent()) {
            if (backpressureFuture == null) {
              backpressureFuture = addVal.get();
            } else {
              Assert.assertSame(backpressureFuture, addVal.get());
            }
          }
        }

        // Backpressure should be exerted right now, since this is a minimal channel with at least one full frame in it.
        Assert.assertNotNull(backpressureFuture);
        Assert.assertFalse(backpressureFuture.isDone());

        channel.doneWriting();
      }

      FrameTestUtil.assertRowsEqual(
          FrameTestUtil.readRowsFromAdapter(adapter, null, false),
          FrameTestUtil.readRowsFromFrameChannel(channel, FrameReader.create(adapter.getRowSignature()))
      );
    }

    @Test
    public void testWriteReadInterleaved() throws IOException
    {
      // Create a frame file.
      final IncrementalIndexStorageAdapter adapter =
          new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());

      final File file = FrameTestUtil.writeFrameFile(
          TestFrameSequenceBuilder.fromAdapter(adapter).maxRowsPerFrame(maxRowsPerFrame).frames(),
          temporaryFolder.newFile()
      );

      final ReadableByteChunksFrameChannel channel = ReadableByteChunksFrameChannel.create("test");
      final BlockingQueueFrameChannel outChannel = new BlockingQueueFrameChannel(10_000); // Enough to hold all frames
      ListenableFuture<?> backpressureFuture = null;

      int iteration = 0;

      try (final Chunker chunker = new Chunker(new FileInputStream(file), chunkSize)) {
        byte[] chunk;

        while ((chunk = chunker.nextChunk()) != null) {
          // Read one frame every 3 iterations. Read everything every 11 iterations. Otherwise, read nothing.
          if (iteration % 3 == 0) {
            while (channel.canRead()) {
              outChannel.write(new FrameWithPartition(channel.read().getOrThrow(), FrameWithPartition.NO_PARTITION));
            }

            // After reading everything, backpressure should be off.
            Assert.assertTrue(backpressureFuture == null || backpressureFuture.isDone());
          } else if (iteration % 11 == 0) {
            if (channel.canRead()) {
              outChannel.write(new FrameWithPartition(channel.read().getOrThrow(), FrameWithPartition.NO_PARTITION));
            }
          }

          if (backpressureFuture != null && backpressureFuture.isDone()) {
            backpressureFuture = null;
          }

          iteration++;

          // Write next chunk.
          final Optional<ListenableFuture<?>> addVal = channel.addChunk(chunk);

          // Minimally-sized channel means backpressure is exerted as soon as a single frame is available.
          Assert.assertEquals(channel.canRead(), addVal.isPresent());

          if (addVal.isPresent()) {
            if (backpressureFuture == null) {
              backpressureFuture = addVal.get();
            } else {
              Assert.assertSame(backpressureFuture, addVal.get());
            }
          }
        }

        channel.doneWriting();

        // Get all the remaining frames.
        while (channel.canRead()) {
          outChannel.write(new FrameWithPartition(channel.read().getOrThrow(), FrameWithPartition.NO_PARTITION));
        }

        outChannel.doneWriting();
      }

      FrameTestUtil.assertRowsEqual(
          FrameTestUtil.readRowsFromAdapter(adapter, null, false),
          FrameTestUtil.readRowsFromFrameChannel(outChannel, FrameReader.create(adapter.getRowSignature()))
      );
    }

    private static class Chunker implements Closeable
    {
      private final FileInputStream in;
      private final int chunkSize;
      private final byte[] buf;
      private boolean eof = false;

      public Chunker(final FileInputStream in, final int chunkSize)
      {
        this.in = in;
        this.chunkSize = chunkSize;
        this.buf = new byte[chunkSize];
      }

      @Nullable
      public byte[] nextChunk() throws IOException
      {
        if (eof) {
          return null;
        }

        int p = 0;
        while (p < chunkSize) {
          final int r = in.read(buf, p, chunkSize - p);

          if (r < 0) {
            eof = true;
            break;
          } else {
            p += r;
          }
        }

        if (p > 0) {
          return Arrays.copyOf(buf, p);
        } else {
          return null;
        }
      }

      @Override
      public void close() throws IOException
      {
        in.close();
      }
    }
  }
}
