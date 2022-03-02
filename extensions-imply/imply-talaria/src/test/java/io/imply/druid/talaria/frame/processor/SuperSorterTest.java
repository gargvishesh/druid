/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.imply.druid.talaria.frame.FrameTestUtil;
import io.imply.druid.talaria.frame.TestFrameSequenceBuilder;
import io.imply.druid.talaria.frame.channel.BlockingQueueFrameChannel;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFileFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableStreamFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import io.imply.druid.talaria.frame.cluster.ClusterByPartition;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollectorImpl;
import io.imply.druid.talaria.frame.file.FrameFile;
import io.imply.druid.talaria.frame.file.FrameFileWriter;
import io.imply.druid.talaria.frame.read.Frame;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.ArenaMemoryAllocator;
import io.imply.druid.talaria.indexing.SuperSorterProgressTracker;
import io.imply.druid.talaria.util.SequenceUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

@RunWith(Enclosed.class)
public class SuperSorterTest
{
  private static final Logger log = new Logger(SuperSorterTest.class);
  private static final int CLUSTER_BY_MAX_KEYS = 50_000;
  private static final int CLUSTER_BY_MAX_BUCKETS = 50_000;

  /**
   * Non-parameterized test cases that
   */
  public static class NonParameterizedCasesTest extends InitializedNullHandlingTest
  {
    private static final int NUM_THREADS = 1;
    private static final int FRAME_SIZE = 1_000_000;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private FrameProcessorExecutor exec;

    @Before
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(NUM_THREADS, "super-sorter-test-%d"))
      );
    }

    @After
    public void tearDown()
    {
      exec.getExecutorService().shutdownNow();
    }

    @Test
    public void testSingleEmptyInputChannel() throws Exception
    {
      final BlockingQueueFrameChannel inputChannel = BlockingQueueFrameChannel.minimal();
      inputChannel.doneWriting();

      final SettableFuture<ClusterByPartitions> outputPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      final SuperSorter superSorter = new SuperSorter(
          Collections.singletonList(inputChannel),
          FrameReader.create(RowSignature.empty()),
          ClusterBy.none(),
          outputPartitionsFuture,
          exec,
          temporaryFolder.newFolder(),
          new FileOutputChannelFactory(temporaryFolder.newFolder(), FRAME_SIZE),
          () -> ArenaMemoryAllocator.createOnHeap(FRAME_SIZE),
          2,
          2,
          -1,
          superSorterProgressTracker
      );

      superSorter.setNoWorkRunnable(() -> outputPartitionsFuture.set(ClusterByPartitions.oneUniversalPartition()));
      final OutputChannels channels = superSorter.run().get();
      Assert.assertEquals(1, channels.getAllChannels().size());

      final ReadableFrameChannel channel = Iterables.getOnlyElement(channels.getAllChannels()).getReadableChannel();
      Assert.assertTrue(channel.isFinished());
      Assert.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);
      channel.doneReading();
    }
  }

  /**
   * Parameterized test cases that use {@link TestIndex#getNoRollupIncrementalTestIndex} with various frame sizes,
   * numbers of channels, and worker configurations.
   *
   * TODO(gianm): make sense of logs like "AllocateDirectMap - A WritableMapHandle was not closed manually"
   * TODO(gianm): also make sense of logs "close() is called more than once on ReferenceCountingCloseableObject"
   */
  @RunWith(Parameterized.class)
  public static class ParameterizedCasesTest extends InitializedNullHandlingTest
  {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final int maxRowsPerFrame;
    private final int maxBytesPerFrame;
    private final int numChannels;
    private final int maxActiveProcessors;
    private final int maxChannelsPerProcessor;
    private final int numThreads;

    private StorageAdapter adapter;
    private RowSignature signature;
    private FrameProcessorExecutor exec;
    private List<ReadableFrameChannel> inputChannels;
    private FrameReader frameReader;

    public ParameterizedCasesTest(
        int maxRowsPerFrame,
        int maxBytesPerFrame,
        int numChannels,
        int maxActiveProcessors,
        int maxChannelsPerProcessor,
        int numThreads
    )
    {
      this.maxRowsPerFrame = maxRowsPerFrame;
      this.maxBytesPerFrame = maxBytesPerFrame;
      this.numChannels = numChannels;
      this.maxActiveProcessors = maxActiveProcessors;
      this.maxChannelsPerProcessor = maxChannelsPerProcessor;
      this.numThreads = numThreads;
    }

    @Parameterized.Parameters(
        name = "maxRowsPerFrame = {0}, "
               + "maxBytesPerFrame = {1}, "
               + "numChannels = {2}, "
               + "maxActiveProcessors = {4}, "
               + "maxChannelsPerProcessor = {3}, "
               + "numThreads = {5}"
    )
    public static Iterable<Object[]> constructorFeeder()
    {
      final List<Object[]> constructors = new ArrayList<>();

      for (int maxRowsPerFrame : new int[]{Integer.MAX_VALUE, 50, 1}) {
        for (int numChannels : new int[]{1, 3}) {
          for (int maxBytesPerFrame : new int[]{20000, 200000}) {
            for (int maxActiveProcessors : new int[]{1, 2, 4}) {
              for (int maxChannelsPerProcessor : new int[]{2, 3, 8}) {
                for (int numThreads : new int[]{1, 3}) {
                  if (maxActiveProcessors >= maxChannelsPerProcessor) {
                    constructors.add(
                        new Object[]{
                            maxRowsPerFrame,
                            maxBytesPerFrame,
                            numChannels,
                            maxActiveProcessors,
                            maxChannelsPerProcessor,
                            numThreads
                        }
                    );
                  }
                }
              }
            }
          }
        }
      }

      return constructors;
    }

    @Before
    public void setUp()
    {
      exec = new FrameProcessorExecutor(
          MoreExecutors.listeningDecorator(Execs.multiThreaded(numThreads, getClass().getSimpleName() + "[%d]"))
      );
      adapter = new QueryableIndexStorageAdapter(TestIndex.getNoRollupMMappedTestIndex());
    }

    @After
    public void tearDown() throws Exception
    {
      if (exec != null) {
        exec.getExecutorService().shutdownNow();
        if (!exec.getExecutorService().awaitTermination(5, TimeUnit.SECONDS)) {
          log.warn("Executor did not terminate after 5 seconds");
        }
      }

      if (inputChannels != null) {
        inputChannels.forEach(ReadableFrameChannel::doneReading);
      }
    }

    /**
     * Creates input channels.
     *
     * Sets {@link #inputChannels}, {@link #signature}, and {@link #frameReader}.
     */
    private void setUpInputChannels(final ClusterBy clusterBy) throws Exception
    {
      if (signature != null || inputChannels != null) {
        throw new ISE("Channels already created for this case");
      }

      final TestFrameSequenceBuilder frameSequenceBuilder =
          TestFrameSequenceBuilder.fromAdapter(adapter)
                                  .maxRowsPerFrame(maxRowsPerFrame)
                                  .sortBy(clusterBy.getColumns())
                                  .allocator(ArenaMemoryAllocator.create(ByteBuffer.allocate(maxBytesPerFrame)))
                                  .populateRowNumber();

      inputChannels = makeFileChannels(frameSequenceBuilder.frames(), temporaryFolder.newFolder(), numChannels);
      signature = frameSequenceBuilder.signature();
      frameReader = FrameReader.create(signature);
    }

    private OutputChannels verifySuperSorter(
        final ClusterBy clusterBy,
        final ClusterByPartitions clusterByPartitions
    ) throws Exception
    {
      final Comparator<ClusterByKey> keyComparator = clusterBy.keyComparator(signature);
      final SettableFuture<ClusterByPartitions> clusterByPartitionsFuture = SettableFuture.create();
      final SuperSorterProgressTracker superSorterProgressTracker = new SuperSorterProgressTracker();

      final SuperSorter superSorter = new SuperSorter(
          inputChannels,
          frameReader,
          clusterBy,
          clusterByPartitionsFuture,
          exec,
          temporaryFolder.newFolder(),
          new FileOutputChannelFactory(temporaryFolder.newFolder(), maxBytesPerFrame),
          () -> ArenaMemoryAllocator.createOnHeap(maxBytesPerFrame),
          maxActiveProcessors / maxChannelsPerProcessor,
          maxChannelsPerProcessor,
          -1,
          superSorterProgressTracker
      );

      superSorter.setNoWorkRunnable(() -> clusterByPartitionsFuture.set(clusterByPartitions));
      final OutputChannels outputChannels = superSorter.run().get();
      Assert.assertEquals(clusterByPartitions.size(), outputChannels.getAllChannels().size());
      Assert.assertEquals(1.0, superSorterProgressTracker.snapshot().getProgressDigest(), 0.0f);

      final int[] clusterByPartColumns = clusterBy.getColumns().stream().mapToInt(
          part -> signature.indexOf(part.columnName())
      ).toArray();

      final List<Sequence<List<Object>>> outputSequences = new ArrayList<>();
      for (int partitionNumber : outputChannels.getPartitionNumbers()) {
        final ClusterByPartition partition = clusterByPartitions.get(partitionNumber);
        final ReadableFrameChannel outputChannel =
            Iterables.getOnlyElement(outputChannels.getChannelsForPartition(partitionNumber)).getReadableChannel();

        // Validate that everything in this channel is in the correct key range.
        SequenceUtils.forEach(
            FrameTestUtil.readRowsFromFrameChannel(
                duplicateOutputChannel(outputChannel),
                frameReader
            ),
            row -> {
              final Object[] array = new Object[clusterByPartColumns.length];
              final ClusterByKey key = ClusterByKey.of(array);

              for (int i = 0; i < array.length; i++) {
                array[i] = row.get(clusterByPartColumns[i]);
              }

              Assert.assertTrue(
                  StringUtils.format("Key %s within partition %s (check start)", key, partition),
                  partition.getStart() == null || keyComparator.compare(key, partition.getStart()) >= 0
              );

              Assert.assertTrue(
                  StringUtils.format("Key %s within partition %s (check end)", key, partition),
                  partition.getEnd() == null || keyComparator.compare(key, partition.getEnd()) < 0
              );
            }
        );

        outputSequences.add(
            FrameTestUtil.readRowsFromFrameChannel(
                duplicateOutputChannel(outputChannel),
                frameReader
            )
        );
      }

      final Sequence<List<Object>> expectedRows = Sequences.sort(
          FrameTestUtil.readRowsFromAdapter(adapter, signature, true),
          Comparator.comparing(
              row -> {
                final Object[] array = new Object[clusterByPartColumns.length];
                final ClusterByKey key = ClusterByKey.of(array);

                for (int i = 0; i < array.length; i++) {
                  array[i] = row.get(clusterByPartColumns[i]);
                }

                return key;
              },
              keyComparator
          )
      );

      FrameTestUtil.assertRowsEqual(expectedRows, Sequences.concat(outputSequences));

      return outputChannels;
    }

    @Test
    public void test_clusterByQualityLongAscRowNumberAsc_onePartition() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new ClusterByColumn("qualityLong", false),
              new ClusterByColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
          ),
          0
      );

      setUpInputChannels(clusterBy);
      verifySuperSorter(clusterBy, ClusterByPartitions.oneUniversalPartition());
    }

    @Test
    public void test_clusterByQualityLongAscRowNumberAsc_twoPartitionsOneEmpty() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new ClusterByColumn("qualityLong", false),
              new ClusterByColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final OutputChannels outputChannels = verifySuperSorter(
          clusterBy,
          new ClusterByPartitions(
              ImmutableList.of(
                  new ClusterByPartition(null, ClusterByKey.of(0L, 0L)), // empty partition
                  new ClusterByPartition(ClusterByKey.of(0L, 0L), null) // all data goes in here
              )
          )
      );

      // Verify that one of the partitions is actually empty.
      Assert.assertEquals(
          0,
          countSequence(
              FrameTestUtil.readRowsFromFrameChannel(
                  Iterables.getOnlyElement(outputChannels.getChannelsForPartition(0)).getReadableChannel(),
                  frameReader
              )
          )
      );

      // Verify that the other partition has all data in it.
      Assert.assertEquals(
          adapter.getNumRows(),
          countSequence(
              FrameTestUtil.readRowsFromFrameChannel(
                  Iterables.getOnlyElement(outputChannels.getChannelsForPartition(1)).getReadableChannel(),
                  frameReader
              )
          )
      );
    }

    @Test
    public void test_clusterByQualityDescRowNumberAsc_fourPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new ClusterByColumn("quality", true),
              new ClusterByColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions =
          gatherClusterByStatistics(adapter, signature, clusterBy)
              .generatePartitionsWithMaxCount(4);

      Assert.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @Test
    public void test_clusterByPlacementishAscRowNumberAsc_fourPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new ClusterByColumn("placementish", true),
              new ClusterByColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions =
          gatherClusterByStatistics(adapter, signature, clusterBy)
              .generatePartitionsWithMaxCount(4);

      Assert.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @Test
    public void test_clusterByQualityLongDescRowNumberAsc_fourPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new ClusterByColumn("qualityLong", true),
              new ClusterByColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions =
          gatherClusterByStatistics(adapter, signature, clusterBy)
              .generatePartitionsWithMaxCount(4);

      Assert.assertEquals(4, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @Test
    public void test_clusterByQualityLongAscRowNumberAsc_oneHundredPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new ClusterByColumn("qualityLong", false),
              new ClusterByColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions =
          gatherClusterByStatistics(adapter, signature, clusterBy)
              .generatePartitionsWithTargetWeight(12);

      Assert.assertEquals(101, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }

    @Test
    public void test_clusterByQualityLongAscRowNumberAsc_maximumPartitions() throws Exception
    {
      final ClusterBy clusterBy = new ClusterBy(
          ImmutableList.of(
              new ClusterByColumn("qualityLong", false),
              new ClusterByColumn(FrameTestUtil.ROW_NUMBER_COLUMN, false)
          ),
          0
      );

      setUpInputChannels(clusterBy);

      final ClusterByPartitions partitions =
          gatherClusterByStatistics(adapter, signature, clusterBy)
              .generatePartitionsWithTargetWeight(1);
      Assert.assertEquals(1142, partitions.size());

      verifySuperSorter(clusterBy, partitions);
    }
  }

  private static List<ReadableFrameChannel> makeFileChannels(
      final Sequence<Frame> frames,
      final File tmpDir,
      final int numChannels
  ) throws IOException
  {
    final List<File> files = new ArrayList<>();
    final List<WritableFrameChannel> writableChannels = new ArrayList<>();

    for (int i = 0; i < numChannels; i++) {
      final File file = new File(tmpDir, StringUtils.format("channel-%d", i));
      files.add(file);
      writableChannels.add(
          new WritableStreamFrameChannel(FrameFileWriter.open(Channels.newChannel(new FileOutputStream(file))))
      );
    }

    SequenceUtils.forEach(
        frames,
        new Consumer<Frame>()
        {
          private int i;

          @Override
          public void accept(final Frame frame)
          {
            try {
              writableChannels.get(i % writableChannels.size())
                              .write(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION));
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }

            i++;
          }
        }
    );

    final List<ReadableFrameChannel> retVal = new ArrayList<>();

    for (int i = 0; i < writableChannels.size(); i++) {
      WritableFrameChannel writableChannel = writableChannels.get(i);
      writableChannel.doneWriting();
      retVal.add(new ReadableFileFrameChannel(FrameFile.open(files.get(i))));
    }

    return retVal;
  }

  private static ClusterByStatisticsCollector gatherClusterByStatistics(
      final StorageAdapter adapter,
      final RowSignature signature,
      final ClusterBy clusterBy
  )
  {
    final ClusterByStatisticsCollector collector = ClusterByStatisticsCollectorImpl.create(
        clusterBy,
        signature,
        CLUSTER_BY_MAX_KEYS,
        CLUSTER_BY_MAX_BUCKETS,
        false
    );

    SequenceUtils.forEach(
        FrameTestUtil.makeCursorsForAdapter(adapter, true),
        cursor -> {
          final Supplier<ClusterByKey> keyReader =
              collector.getClusterBy().keyReader(cursor.getColumnSelectorFactory(), signature);

          while (!cursor.isDone()) {
            collector.add(keyReader.get(), 1);
            cursor.advance();
          }
        }
    );

    return collector;
  }

  private static ReadableFrameChannel duplicateOutputChannel(final ReadableFrameChannel channel)
  {
    return new ReadableFileFrameChannel(((ReadableFileFrameChannel) channel).getFrameFileReference());
  }

  private static <T> long countSequence(final Sequence<T> sequence)
  {
    return sequence.accumulate(
        0L,
        (accumulated, in) -> accumulated + 1
    );
  }
}
