/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.file;

import com.google.common.math.IntMath;
import io.imply.druid.talaria.TestArrayStorageAdapter;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFileFrameChannel;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.testutil.FrameSequenceBuilder;
import io.imply.druid.talaria.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexStorageAdapter;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * TODO(gianm): test for frame file > 2GB
 */
@RunWith(Parameterized.class)
public class FrameFileTest extends InitializedNullHandlingTest
{
  // Partition every 99 rows if "partitioned" is true.
  private static final int PARTITION_SIZE = 99;

  // Skip unlucky partition #13.
  private static final int SKIP_PARTITION = 13;

  enum AdapterType
  {
    INCREMENTAL {
      @Override
      StorageAdapter getAdapter()
      {
        return new IncrementalIndexStorageAdapter(TestIndex.getNoRollupIncrementalTestIndex());
      }
    },
    MMAP {
      @Override
      StorageAdapter getAdapter()
      {
        return new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());
      }
    },
    MV_AS_STRING_ARRAYS {
      @Override
      StorageAdapter getAdapter()
      {
        return new TestArrayStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());
      }
    };

    abstract StorageAdapter getAdapter();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final FrameType frameType;
  private final int maxRowsPerFrame;
  private final boolean partitioned;
  private final AdapterType adapterType;
  private final FrameFile.Flag openMode;

  private StorageAdapter adapter;
  private File file;

  public FrameFileTest(
      final FrameType frameType,
      final int maxRowsPerFrame,
      final boolean partitioned,
      final AdapterType adapterType,
      final FrameFile.Flag openMode
  )
  {
    this.frameType = frameType;
    this.maxRowsPerFrame = maxRowsPerFrame;
    this.partitioned = partitioned;
    this.adapterType = adapterType;
    this.openMode = openMode;
  }

  @Parameterized.Parameters(
      name = "frameType = {0}, "
             + "maxRowsPerFrame = {1}, "
             + "partitioned = {2}, "
             + "adapter = {3}, "
             + "openMode = {4}"
  )
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    final FrameFile.Flag[] openModes = {
        FrameFile.Flag.BB_MEMORY_MAP,
        FrameFile.Flag.DS_MEMORY_MAP
    };

    for (FrameType frameType : FrameType.values()) {
      for (int maxRowsPerFrame : new int[]{1, 17, 50, PARTITION_SIZE, Integer.MAX_VALUE}) {
        for (boolean partitioned : new boolean[]{true, false}) {
          for (AdapterType adapterType : AdapterType.values()) {
            for (FrameFile.Flag openMode : openModes) {
              constructors.add(new Object[]{frameType, maxRowsPerFrame, partitioned, adapterType, openMode});
            }
          }
        }
      }
    }

    return constructors;
  }

  @Before
  public void setUp() throws IOException
  {
    adapter = adapterType.getAdapter();

    if (partitioned) {
      // Partition every PARTITION_SIZE rows.
      file = FrameTestUtil.writeFrameFileWithPartitions(
          FrameSequenceBuilder.fromAdapter(adapter).frameType(frameType).maxRowsPerFrame(maxRowsPerFrame).frames().map(
              new Function<Frame, FrameWithPartition>()
              {
                private int rows = 0;

                @Override
                public FrameWithPartition apply(final Frame frame)
                {
                  final int partitionNum = rows / PARTITION_SIZE;
                  rows += frame.numRows();
                  return new FrameWithPartition(
                      frame,
                      partitionNum >= SKIP_PARTITION ? partitionNum + 1 : partitionNum
                  );
                }
              }
          ),
          temporaryFolder.newFile()
      );

    } else {
      file = FrameTestUtil.writeFrameFile(
          FrameSequenceBuilder.fromAdapter(adapter).frameType(frameType).maxRowsPerFrame(maxRowsPerFrame).frames(),
          temporaryFolder.newFile()
      );
    }
  }

  @Test
  public void test_numFrames() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      Assert.assertEquals(computeExpectedNumFrames(), frameFile.numFrames());
    }
  }

  @Test
  public void test_numPartitions() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      Assert.assertEquals(computeExpectedNumPartitions(), frameFile.numPartitions());
    }
  }

  @Test
  public void test_frame_first() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      final Frame firstFrame = frameFile.frame(0);
      Assert.assertEquals(Math.min(adapter.getNumRows(), maxRowsPerFrame), firstFrame.numRows());
    }
  }

  @Test
  public void test_frame_last() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      final Frame lastFrame = frameFile.frame(frameFile.numFrames() - 1);
      Assert.assertEquals(
          adapter.getNumRows() % maxRowsPerFrame != 0
          ? adapter.getNumRows() % maxRowsPerFrame
          : Math.min(adapter.getNumRows(), maxRowsPerFrame),
          lastFrame.numRows()
      );
    }
  }

  @Test
  public void test_frame_outOfBoundsNegative() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage("Frame [-1] out of bounds");
      frameFile.frame(-1);
    }
  }

  @Test
  public void test_frame_outOfBoundsTooLarge() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      expectedException.expect(IllegalArgumentException.class);
      expectedException.expectMessage(StringUtils.format("Frame [%,d] out of bounds", frameFile.numFrames()));
      frameFile.frame(frameFile.numFrames());
    }
  }

  @Test
  public void test_frame_readAllDataViaFrameChannel() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      final Sequence<List<Object>> frameFileRows = FrameTestUtil.readRowsFromFrameChannel(
          new ReadableFileFrameChannel(frameFile.newReference()),
          FrameReader.create(adapter.getRowSignature())
      );

      final Sequence<List<Object>> adapterRows = FrameTestUtil.readRowsFromAdapter(adapter, null, false);
      FrameTestUtil.assertRowsEqual(adapterRows, frameFileRows);
    }
  }

  @Test
  public void test_getPartitionStartFrame() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      if (partitioned) {
        for (int partitionNum = 0; partitionNum < frameFile.numPartitions(); partitionNum++) {
          Assert.assertEquals(
              "partition #" + partitionNum,
              Math.min(
                  IntMath.divide(
                      (partitionNum >= SKIP_PARTITION ? partitionNum + 1 : partitionNum) * PARTITION_SIZE,
                      maxRowsPerFrame,
                      RoundingMode.CEILING
                  ),
                  frameFile.numFrames()
              ),
              frameFile.getPartitionStartFrame(partitionNum)
          );
        }
      } else {
        Assert.assertEquals(frameFile.numFrames(), frameFile.getPartitionStartFrame(0));
      }
    }
  }

  @Test
  public void test_file() throws IOException
  {
    try (final FrameFile frameFile = FrameFile.open(file, openMode)) {
      Assert.assertEquals(file, frameFile.file());
    }
  }

  @Test
  public void test_open_withDeleteOnClose() throws IOException
  {
    FrameFile.open(file, openMode).close();
    Assert.assertTrue(file.exists());

    FrameFile.open(file, FrameFile.Flag.DELETE_ON_CLOSE).close();
    Assert.assertFalse(file.exists());
  }

  @Test
  public void test_newReference() throws IOException
  {
    final FrameFile frameFile1 = FrameFile.open(file, FrameFile.Flag.DELETE_ON_CLOSE);
    final FrameFile frameFile2 = frameFile1.newReference();
    final FrameFile frameFile3 = frameFile2.newReference();

    // Closing original file does nothing; must wait for other files to be closed.
    frameFile1.close();
    Assert.assertTrue(file.exists());

    // Can still get a reference after frameFile1 is closed, just because others are still open. Strange but true.
    final FrameFile frameFile4 = frameFile1.newReference();

    // Repeated calls to "close" are deduped.
    frameFile2.close();
    frameFile2.close();
    frameFile2.close();
    frameFile2.close();
    frameFile2.close();
    frameFile2.close();
    Assert.assertTrue(file.exists());

    frameFile3.close();
    Assert.assertTrue(file.exists());

    // Final reference is closed; file is now gone.
    frameFile4.close();
    Assert.assertFalse(file.exists());

    // Can no longer get new references.
    expectedException.expect(IllegalStateException.class);
    expectedException.expectMessage("Frame file is closed");
    frameFile1.newReference();
  }

  private int computeExpectedNumFrames()
  {
    return IntMath.divide(adapter.getNumRows(), maxRowsPerFrame, RoundingMode.CEILING);
  }

  private int computeExpectedNumPartitions()
  {
    if (partitioned) {
      return Math.min(
          computeExpectedNumFrames(),
          IntMath.divide(adapter.getNumRows(), PARTITION_SIZE, RoundingMode.CEILING)
      );
    } else {
      // 0 = not partitioned.
      return 0;
    }
  }
}
