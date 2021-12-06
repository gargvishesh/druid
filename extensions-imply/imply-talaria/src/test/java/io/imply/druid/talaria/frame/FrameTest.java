/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.read.Frame;
import org.apache.datasketches.memory.MapHandle;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ByteBufferUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

@RunWith(Enclosed.class)
public class FrameTest
{
  // Tests that use good frames built from a standard test file.
  public static class GoodFrames extends InitializedNullHandlingTest
  {
    private Frame frame;
    private Frame sortedFrame;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp()
    {
      final StorageAdapter adapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());

      final List<ClusterByColumn> sortBy = ImmutableList.of(
          new ClusterByColumn("quality", true),
          new ClusterByColumn("__time", false)
      );

      frame = Iterables.getOnlyElement(TestFrameSequenceBuilder.fromAdapter(adapter).frames().toList());
      sortedFrame =
          Iterables.getOnlyElement(TestFrameSequenceBuilder.fromAdapter(adapter).sortBy(sortBy).frames().toList());
    }

    @Test
    public void test_numRows()
    {
      Assert.assertEquals(1209, frame.numRows());
      Assert.assertEquals(1209, sortedFrame.numRows());
    }

    @Test
    public void test_physicalRow_standard()
    {
      for (int i = 0; i < frame.numRows(); i++) {
        Assert.assertEquals(i, frame.physicalRow(i));
      }
    }

    @Test
    public void test_physicalRow_standard_outOfBoundsTooLow()
    {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Row [-1] out of bounds");
      frame.physicalRow(-1);
    }

    @Test
    public void test_physicalRow_standard_outOfBoundsTooHigh()
    {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Row [1,209] out of bounds");
      frame.physicalRow(Ints.checkedCast(frame.numRows()));
    }

    @Test
    public void test_physicalRow_sorted_outOfBoundsTooLow()
    {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Row [-1] out of bounds");
      sortedFrame.physicalRow(-1);
    }

    @Test
    public void test_physicalRow_sorted_outOfBoundsTooHigh()
    {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Row [1,209] out of bounds");
      sortedFrame.physicalRow(Ints.checkedCast(frame.numRows()));
    }
  }

  // Tests that explore "wrap", "decompress", and "writeTo" with different kinds of backing memory.
  @RunWith(Parameterized.class)
  public static class WrapAndWrite extends InitializedNullHandlingTest
  {
    private static byte[] FRAME_DATA;
    private static byte[] FRAME_DATA_COMPRESSED;

    enum MemType
    {
      ARRAY {
        @Override
        Frame wrap(Closer closer)
        {
          return Frame.wrap(FRAME_DATA);
        }

        @Override
        Frame decompress(Closer closer)
        {
          return Frame.decompress(Memory.wrap(FRAME_DATA_COMPRESSED), 0, FRAME_DATA_COMPRESSED.length);
        }
      },
      BB_HEAP {
        @Override
        Frame wrap(Closer closer)
        {
          return Frame.wrap(ByteBuffer.wrap(FRAME_DATA));
        }

        @Override
        Frame decompress(Closer closer)
        {
          return Frame.decompress(
              Memory.wrap(ByteBuffer.wrap(FRAME_DATA_COMPRESSED), ByteOrder.LITTLE_ENDIAN),
              0,
              FRAME_DATA_COMPRESSED.length
          );
        }
      },
      BB_DIRECT {
        @Override
        Frame wrap(Closer closer)
        {
          final ByteBuffer buf = ByteBuffer.allocateDirect(FRAME_DATA.length);
          buf.put(FRAME_DATA, 0, FRAME_DATA.length);
          closer.register(() -> ByteBufferUtils.free(buf));
          return Frame.wrap(buf);
        }

        @Override
        Frame decompress(Closer closer)
        {
          final ByteBuffer buf = ByteBuffer.allocateDirect(FRAME_DATA_COMPRESSED.length);
          buf.put(FRAME_DATA_COMPRESSED, 0, FRAME_DATA_COMPRESSED.length);
          closer.register(() -> ByteBufferUtils.free(buf));
          return Frame.decompress(Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN), 0, FRAME_DATA_COMPRESSED.length);
        }
      },
      BB_MAPPED {
        @Override
        Frame wrap(Closer closer) throws IOException
        {
          final File file = File.createTempFile("frame-test", "");
          closer.register(file::delete);
          Files.write(FRAME_DATA, file);
          final MappedByteBuffer buf = Files.map(file);
          closer.register(() -> ByteBufferUtils.unmap(buf));
          return Frame.wrap(buf);
        }

        @Override
        Frame decompress(Closer closer) throws IOException
        {
          final File file = File.createTempFile("frame-test", "");
          closer.register(file::delete);
          Files.write(FRAME_DATA_COMPRESSED, file);
          final MappedByteBuffer buf = Files.map(file);
          closer.register(() -> ByteBufferUtils.unmap(buf));
          return Frame.decompress(Memory.wrap(buf, ByteOrder.LITTLE_ENDIAN), 0, buf.capacity());
        }
      },
      MEMORY_ARRAY {
        @Override
        Frame wrap(Closer closer)
        {
          return Frame.wrap(Memory.wrap(FRAME_DATA, ByteOrder.LITTLE_ENDIAN));
        }

        @Override
        Frame decompress(Closer closer)
        {
          // Offset by 1 to make sure we handle memory regions properly.
          final byte[] copyFrameData = new byte[FRAME_DATA_COMPRESSED.length + 2];
          System.arraycopy(FRAME_DATA_COMPRESSED, 0, copyFrameData, 1, FRAME_DATA_COMPRESSED.length);
          return Frame.decompress(
              Memory.wrap(copyFrameData, ByteOrder.LITTLE_ENDIAN),
              1,
              FRAME_DATA_COMPRESSED.length
          );
        }
      },
      MEMORY_BB {
        @Override
        Frame wrap(Closer closer)
        {
          return Frame.wrap(Memory.wrap(ByteBuffer.wrap(FRAME_DATA), ByteOrder.LITTLE_ENDIAN));
        }

        @Override
        Frame decompress(Closer closer)
        {
          // Offset by 1 to make sure we handle memory regions properly.
          final byte[] copyFrameData = new byte[FRAME_DATA_COMPRESSED.length + 2];
          System.arraycopy(FRAME_DATA_COMPRESSED, 0, copyFrameData, 1, FRAME_DATA_COMPRESSED.length);
          return Frame.decompress(
              Memory.wrap(
                  ByteBuffer.wrap(copyFrameData),
                  ByteOrder.LITTLE_ENDIAN
              ),
              1,
              FRAME_DATA_COMPRESSED.length
          );
        }
      },
      MEMORY_FILE {
        @Override
        Frame wrap(Closer closer) throws IOException
        {
          final File file = File.createTempFile("frame-test", "");
          closer.register(file::delete);
          Files.write(FRAME_DATA, file);
          final MapHandle mapHandle = Memory.map(file);
          closer.register(
              () -> {
                try {
                  mapHandle.close();
                }
                catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
          );
          return Frame.wrap(mapHandle.get());
        }

        @Override
        Frame decompress(Closer closer) throws IOException
        {
          final File file = File.createTempFile("frame-test", "");
          closer.register(file::delete);
          Files.write(FRAME_DATA_COMPRESSED, file);
          final MapHandle mapHandle = Memory.map(file);
          closer.register(
              () -> {
                try {
                  mapHandle.close();
                }
                catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
          );
          return Frame.decompress(mapHandle.get(), 0, FRAME_DATA_COMPRESSED.length);
        }
      };

      abstract Frame wrap(Closer closer) throws IOException;

      abstract Frame decompress(Closer closer) throws IOException;
    }

    private final MemType memType;
    private final boolean compressed;
    private final Closer closer = Closer.create();

    public WrapAndWrite(final MemType memType, final boolean compressed)
    {
      this.memType = memType;
      this.compressed = compressed;
    }

    @Parameterized.Parameters(name = "memType = {0}, compressed = {1}")
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (MemType memType : MemType.values()) {
        for (boolean compressed : new boolean[]{true, false}) {
          constructors.add(new Object[]{memType, compressed});
        }
      }

      return constructors;
    }

    @BeforeClass
    public static void setUpClass() throws Exception
    {
      final StorageAdapter adapter = new IncrementalIndexStorageAdapter(TestIndex.getIncrementalTestIndex());
      final Frame frame =
          Iterables.getOnlyElement(TestFrameSequenceBuilder.fromAdapter(adapter).frames().toList());
      FRAME_DATA = frameToByteArray(frame, false);
      FRAME_DATA_COMPRESSED = frameToByteArray(frame, true);
    }

    @AfterClass
    public static void tearDownClass()
    {
      FRAME_DATA = null;
      FRAME_DATA_COMPRESSED = null;
    }

    @After
    public void tearDown() throws IOException
    {
      closer.close();
    }

    @Test
    public void testWrapAndWrite() throws Exception
    {
      final Frame frame = compressed ? memType.decompress(closer) : memType.wrap(closer);

      // And write.
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      frame.writeTo(Channels.newChannel(baos), compressed);

      if (!compressed) {
        Assert.assertArrayEquals(FRAME_DATA, baos.toByteArray());
      } else {
        // Decompress and check.
        final byte[] compressedData = baos.toByteArray();
        final Frame frame2 = Frame.decompress(Memory.wrap(baos.toByteArray()), 0, compressedData.length);
        Assert.assertArrayEquals(FRAME_DATA, frameToByteArray(frame2, false));
      }
    }
  }

  // Tests that explore behavior with various bad frames.
  public static class BadFrames
  {
    @Test
    public void testToDo()
    {
      // TODO(gianm): Add tests
    }
  }

  private static byte[] frameToByteArray(final Frame frame, final boolean compressed) throws Exception
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    frame.writeTo(Channels.newChannel(baos), compressed);
    return baos.toByteArray();
  }
}
