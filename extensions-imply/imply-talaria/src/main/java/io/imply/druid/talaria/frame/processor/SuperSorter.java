/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.MemoryAllocator;
import io.imply.druid.talaria.frame.channel.BlockingQueueFrameChannel;
import io.imply.druid.talaria.frame.channel.FrameWithPartition;
import io.imply.druid.talaria.frame.channel.ReadableFileFrameChannel;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.Try;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableStreamFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.file.FrameFile;
import io.imply.druid.talaria.frame.file.FrameFileWriter;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.write.FrameWriters;
import io.imply.druid.talaria.indexing.SuperSorterProgressTracker;
import io.imply.druid.talaria.util.TalariaFutureUtils;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * This is an implementation of off-memory on-disk n-way merge sort.
 *
 * Given a list of inputChannels that contains sorted frame channels, the main method of this class attempts to merge
 * them all s.t. the resultant output is sorted as well. IMPORTANT NOTE: The channels should produce frames that need
 * to be individually sorted. There isn't an ordering requirement between the different frames of an input channel or
 * between channels themselves.
 *
 * The output of this sort method is then broken down into chunks depending on the outputPartitions that is supplied
 * to it and each chunk is then written to an outputChannel which is created from outputChannelFactory
 */
public class SuperSorter
{
  private static final Logger log = new Logger(SuperSorter.class);
  public static final int UNKNOWN_LEVEL = -1;
  public static final long UNKNOWN_TOTAL = -1;

  /**
   * Input Channels that contain individually sorted frames that need to be merged
   */
  private final List<ReadableFrameChannel> inputChannels;

  /**
   * Reader used to read the frames from the channels
   */
  private final FrameReader frameReader;

  /**
   * The columns on which the sort ordering is determined. For example, given frames with col1,col2,col3,col4 and
   * clusterBy as col4,col3, the sorting would be done on the tuple (col4, col3)
   */
  private final ClusterBy clusterBy;

  /**
   * Resolves to ClusterBy partitions which determine the cuts that need to be made in the final sorted output stream
   * of data. This is supplied as a future so that the computation of the partitions can be done in parallel by the
   * tasks while the lower levels of the sorting is taking place
   */
  private final ListenableFuture<ClusterByPartitions> outputPartitionsFuture;

  /**
   * Executor used to actually merge the individually sorted frames
   */
  private final FrameProcessorExecutor exec;

  /**
   * Temporary directory for intermediate merge outputs
   */
  private final File directory;

  /**
   * Factory that is used to produce output channels corresponding to each outputPartition
   */
  private final OutputChannelFactory outputChannelFactory;

  // Parameters and variables for managing memory and number of individual mergers running at a time
  private final Supplier<MemoryAllocator> innerFrameAllocatorMaker;
  private final int maxChannelsPerProcessor;
  private final int maxActiveProcessors;
  private final long rowLimit;

  /**
   * Can be used to cancel the work being executed by the SuperSorter. This must be associated with the FrameProcessorExectuor
   * passed to it
   */
  private final String cancellationId;

  private final Object runWorkersLock = new Object();

  @GuardedBy("runWorkersLock")
  private boolean batcherIsRunning = false;

  @GuardedBy("runWorkersLock")
  private IntSet inputChannelsToRead = new IntOpenHashSet();

  @GuardedBy("runWorkersLock")
  private final Int2ObjectMap<LongSortedSet> outputsReadyByLevel = new Int2ObjectArrayMap<>();

  @GuardedBy("runWorkersLock")
  private List<OutputChannel> outputChannels = null;

  @GuardedBy("runWorkersLock")
  private int activeProcessors = 0;

  @GuardedBy("runWorkersLock")
  private long totalInputFrames = UNKNOWN_TOTAL;

  @GuardedBy("runWorkersLock")
  private int totalMergingLevels = UNKNOWN_LEVEL;

  @GuardedBy("runWorkersLock")
  private final Queue<Frame> inputBuffer = new ArrayDeque<>();

  @GuardedBy("runWorkersLock")
  private long inputFramesReadSoFar = 0;

  @GuardedBy("runWorkersLock")
  private long levelZeroMergersRunSoFar = 0;

  @GuardedBy("runWorkersLock")
  private int ultimateMergersRunSoFar = 0;

  @GuardedBy("runWorkersLock")
  private final Map<File, FrameFile> penultimateFrameFileCache = new HashMap<>();

  @GuardedBy("runWorkersLock")
  private SettableFuture<OutputChannels> allDone = null;

  @GuardedBy("runWorkersLock")
  SuperSorterProgressTracker superSorterProgressTracker;

  /**
   * See {@link #setNoWorkRunnable}.
   */
  @GuardedBy("runWorkersLock")
  private Runnable noWorkRunnable = null;

  public SuperSorter(
      final List<ReadableFrameChannel> inputChannels,
      final FrameReader frameReader,
      final ClusterBy clusterBy,
      final ListenableFuture<ClusterByPartitions> outputPartitionsFuture,
      final FrameProcessorExecutor exec,
      final File directory,
      final OutputChannelFactory outputChannelFactory,
      final Supplier<MemoryAllocator> innerFrameAllocatorMaker,
      final int maxActiveProcessors,
      final int maxChannelsPerProcessor,
      final long rowLimit,
      @Nullable final String cancellationId,
      @Nonnull SuperSorterProgressTracker superSorterProgressTracker
  )
  {
    this.inputChannels = inputChannels;
    this.frameReader = frameReader;
    this.clusterBy = clusterBy;
    this.outputPartitionsFuture = outputPartitionsFuture;
    this.exec = exec;
    this.directory = directory;
    this.outputChannelFactory = outputChannelFactory;
    this.innerFrameAllocatorMaker = innerFrameAllocatorMaker;
    this.maxChannelsPerProcessor = maxChannelsPerProcessor;
    this.maxActiveProcessors = maxActiveProcessors;
    this.rowLimit = rowLimit;
    this.cancellationId = cancellationId;
    this.superSorterProgressTracker = superSorterProgressTracker;

    for (int i = 0; i < inputChannels.size(); i++) {
      inputChannelsToRead.add(i);
    }

    if (maxActiveProcessors < 1) {
      throw new IAE("maxActiveProcessors[%d] < 1", maxActiveProcessors);
    }

    if (maxChannelsPerProcessor < 2) {
      throw new IAE("maxChannelsPerProcessor[%d] < 2", maxChannelsPerProcessor);
    }
  }

  public ListenableFuture<OutputChannels> run()
  {
    synchronized (runWorkersLock) {
      if (allDone == null) {
        allDone = SettableFuture.create();
        runWorkersIfPossible();

        // When output partitions become known, that may unblock some additional layers of merging.
        outputPartitionsFuture.addListener(
            () -> {
              synchronized (runWorkersLock) {
                if (outputPartitionsFuture.isDone()) { // Update the progress tracker
                  superSorterProgressTracker.setTotalMergersForUltimateLevel(getOutputPartitions().size());
                }
                runWorkersIfPossible();
                setAllDoneIfPossible();
              }
            },
            exec.getExecutorService()
        );
      }

      return TalariaFutureUtils.futureWithBaggage(
          allDone,
          () -> {
            synchronized (runWorkersLock) {
              if (activeProcessors == 0) {
                cleanUp();
              }
            }
          }
      );
    }
  }

  /**
   * Sets a callback that enables tests to see when this SuperSorter cannot do any work. Only used for testing.
   */
  @VisibleForTesting
  void setNoWorkRunnable(final Runnable runnable)
  {
    synchronized (runWorkersLock) {
      this.noWorkRunnable = runnable;
    }
  }

  /**
   * Called when a worker finishes.
   */
  @GuardedBy("runWorkersLock")
  private void workerFinished()
  {
    activeProcessors -= 1;

    if (log.isDebugEnabled()) {
      log.debug(stateString());
    }

    runWorkersIfPossible();
    setAllDoneIfPossible();

    if (isAllDone() && activeProcessors == 0) {
      cleanUp();
    }
  }

  /**
   * Tries to launch a new worker, and returns whether it was doable.
   *
   * Later workers have priority, i.e., those responsible for merging higher levels of the merge tree. Workers that
   * read the original input channels have the lowest priority. This priority order ensures that we don't build up
   * too much unmerged data.
   */
  @GuardedBy("runWorkersLock")
  private void runWorkersIfPossible()
  {
    if (isAllDone()) {
      // Do nothing if the instance is all done. This can happen in case of error or cancelation.
      return;
    }

    try {
      while (activeProcessors < maxActiveProcessors &&
             (runNextUltimateMerger() || runNextMiddleMerger() || runNextLevelZeroMerger() || runNextBatcher())) {
        activeProcessors += 1;

        if (log.isDebugEnabled()) {
          log.debug(stateString());
        }
      }

      if (activeProcessors == 0 && noWorkRunnable != null) {
        log.debug("No active workers and no work left to start.");

        // Only called in tests. No need to bother with try/catch and such.
        noWorkRunnable.run();
      }
    }
    catch (Throwable e) {
      allDone.setException(e);
    }
  }

  @GuardedBy("runWorkersLock")
  private void setAllDoneIfPossible()
  {
    if (totalInputFrames == 0 && outputPartitionsFuture.isDone()) {
      // No input data -- generate empty output channels.
      final ClusterByPartitions partitions = getOutputPartitions();
      final List<OutputChannel> channels = new ArrayList<>(partitions.size());

      for (int partitionNum = 0; partitionNum < partitions.size(); partitionNum++) {
        channels.add(outputChannelFactory.openNilChannel(partitionNum));
      }

      // OK to use wrap, not wrapReadOnly, because nil channels are already read-only.
      allDone.set(OutputChannels.wrap(channels));
    } else if (totalMergingLevels != UNKNOWN_LEVEL
               && outputsReadyByLevel.containsKey(totalMergingLevels - 1)
               && outputsReadyByLevel.get(totalMergingLevels - 1).size() == getOutputPartitions().size()) {
      // We're done!!
      try {
        // OK to use wrap, not wrapReadOnly, because all channels in this list are already read-only.
        allDone.set(OutputChannels.wrap(outputChannels));
      }
      catch (Throwable e) {
        allDone.setException(e);
      }
    }
  }

  @GuardedBy("runWorkersLock")
  private boolean runNextBatcher()
  {
    if (batcherIsRunning || inputChannelsToRead.isEmpty()) {
      return false;
    } else {
      batcherIsRunning = true;

      runWorker(
          new FrameChannelBatcher(inputChannels, maxChannelsPerProcessor),
          result -> {
            final List<Frame> batch = result.lhs;
            final IntSet keepReading = result.rhs;

            synchronized (runWorkersLock) {
              inputBuffer.addAll(batch);
              inputFramesReadSoFar += batch.size();
              inputChannelsToRead = keepReading;

              if (inputChannelsToRead.isEmpty()) {
                inputChannels.forEach(ReadableFrameChannel::doneReading);
                setTotalInputFrames(inputFramesReadSoFar);
                runWorkersIfPossible();
              } else if (inputBuffer.size() >= maxChannelsPerProcessor) {
                runWorkersIfPossible();
              }

              batcherIsRunning = false;
            }
          }
      );

      return true;
    }
  }

  /**
   * Level zero mergers read batches of frames from the "inputBuffer". These frames are individually sorted, but there
   * is no ordering between the frames. Their output is a sorted sequence of frames.
   */
  @GuardedBy("runWorkersLock")
  private boolean runNextLevelZeroMerger()
  {
    if (inputBuffer.isEmpty() || (inputBuffer.size() < maxChannelsPerProcessor && !allInputRead())) {
      return false;
    }

    final List<ReadableFrameChannel> in = new ArrayList<>();

    while (in.size() < maxChannelsPerProcessor) {
      final Frame frame = inputBuffer.poll();

      if (frame == null) {
        break;
      }

      in.add(singleReadableFrameChannel(new FrameWithPartition(frame, FrameWithPartition.NO_PARTITION)));
    }

    runMerger(0, levelZeroMergersRunSoFar++, in, null);
    return true;
  }

  @GuardedBy("runWorkersLock")
  private boolean runNextMiddleMerger()
  {
    for (int inLevel = outputsReadyByLevel.size() - 1; inLevel >= 0; inLevel--) {
      final int outLevel = inLevel + 1;
      final long totalInputs = getTotalMergersInLevel(inLevel);
      final LongSortedSet inputsReady = outputsReadyByLevel.get(inLevel);

      if (totalMergingLevels != UNKNOWN_LEVEL && outLevel >= totalMergingLevels - 1) {
        // This is the ultimate level. Skip it, since it will be launched by runNextUltimateMerger.
        continue;
      }

      if (totalMergingLevels == UNKNOWN_LEVEL
          && LongMath.divide(inputsReady.size(), maxChannelsPerProcessor, RoundingMode.CEILING)
             <= maxChannelsPerProcessor) {
        // This *might* be the penultimate level. Skip until we know for sure. (i.e., until all input frames have
        // been read.)
        continue;
      }

      final ClusterByPartitions outPartitions;

      if (totalMergingLevels != UNKNOWN_LEVEL && outLevel == totalMergingLevels - 2) {
        // This is the penultimate level.
        if (!outputPartitionsFuture.isDone()) {
          // Can't launch penultimate level until output partitions are known.
          continue;
        }

        outPartitions = getOutputPartitions();
      } else {
        outPartitions = null;
      }

      // See if there's work to do.

      final LongIterator iter = inputsReady.iterator();

      long currentSetStart = -1, currentSetIndex = -1;
      while (iter.hasNext()) {
        final long w = iter.nextLong();
        if (w % maxChannelsPerProcessor == 0) {
          // w is the start of a set
          currentSetStart = w;
          currentSetIndex = -1;
        }

        if (currentSetStart >= 0) {
          // We're currently exploring a potential set.
          long pos = w - currentSetStart;

          if (pos == currentSetIndex + 1 &&
              (pos == maxChannelsPerProcessor - 1 || (totalInputs != UNKNOWN_TOTAL && w == totalInputs - 1))) {
            // We found a set to merge. Let's collect the input channels and launch the merger.
            final List<ReadableFrameChannel> in = new ArrayList<>();
            for (long i = currentSetStart; i < currentSetStart + maxChannelsPerProcessor; i++) {
              if (inputsReady.remove(i)) {
                try {
                  final FrameFile handle = FrameFile.open(mergerOutputFile(inLevel, i), FrameFile.Flag.DELETE_ON_CLOSE);
                  in.add(new ReadableFileFrameChannel(handle));
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }
            }

            runMerger(outLevel, currentSetStart / maxChannelsPerProcessor, in, outPartitions);
            return true;
          } else if (w == currentSetStart + currentSetIndex + 1) {
            currentSetIndex++;
          } else {
            currentSetStart = -1;
            currentSetIndex = -1;
          }
        }
      }
    }

    // Nothing to merge (yet?).
    return false;
  }

  @GuardedBy("runWorkersLock")
  private boolean runNextUltimateMerger()
  {
    if (totalMergingLevels == UNKNOWN_LEVEL
        || !outputPartitionsFuture.isDone()
        || ultimateMergersRunSoFar >= getOutputPartitions().size()) {
      return false;
    }

    final int inLevel = totalMergingLevels - 2;
    final int outLevel = inLevel + 1;
    final LongSortedSet inputsReady = outputsReadyByLevel.get(inLevel);

    if (inputsReady == null) {
      return false;
    }

    final int numInputs = inputsReady.size();

    if (numInputs != getTotalMergersInLevel(inLevel)) {
      return false;
    }

    final List<ReadableFrameChannel> in = new ArrayList<>(numInputs);

    for (long i = 0; i < numInputs; i++) {
      final FrameFile fileHandle = penultimateFrameFileCache.computeIfAbsent(
          mergerOutputFile(inLevel, i),
          file -> {
            try {
              return FrameFile.open(file, FrameFile.Flag.DELETE_ON_CLOSE);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      ).newReference();

      in.add(
          new ReadableFileFrameChannel(
              fileHandle,
              fileHandle.getPartitionStartFrame(ultimateMergersRunSoFar),
              fileHandle.getPartitionStartFrame(ultimateMergersRunSoFar + 1)
          )
      );
    }

    if (outputChannels == null) {
      outputChannels = Arrays.asList(new OutputChannel[getOutputPartitions().size()]);
    }

    runMerger(outLevel, ultimateMergersRunSoFar, in, null);
    ultimateMergersRunSoFar++;
    return true;
  }

  @GuardedBy("runWorkersLock")
  private void runMerger(
      final int level,
      final long rank,
      final List<ReadableFrameChannel> in,
      @Nullable final ClusterByPartitions partitions
  )
  {
    try {
      final WritableFrameChannel writableChannel;
      final MemoryAllocator frameAllocator;

      if (totalMergingLevels != UNKNOWN_LEVEL && level == totalMergingLevels - 1) {
        final int intRank = Ints.checkedCast(rank);
        final OutputChannel outputChannel = outputChannelFactory.openChannel(intRank);
        outputChannels.set(intRank, outputChannel.readOnly());
        writableChannel = outputChannel.getWritableChannel();
        frameAllocator = outputChannel.getFrameMemoryAllocator();
      } else {
        writableChannel = new WritableStreamFrameChannel(
            FrameFileWriter.open(
                Files.newByteChannel(
                    mergerOutputFile(level, rank).toPath(),
                    StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE
                )
            )
        );
        frameAllocator = innerFrameAllocatorMaker.get();
      }

      final FrameChannelMerger worker =
          new FrameChannelMerger(
              in,
              frameReader,
              writableChannel,
              FrameWriters.makeFrameWriterFactory(
                  FrameType.ROW_BASED, // Row-based frames are generally preferred as inputs to mergers
                  frameAllocator,
                  frameReader.signature(),

                  // No sortColumns, because FrameChannelMerger generates frames that are sorted all on its own
                  Collections.emptyList()
              ),
              clusterBy,
              partitions,
              rowLimit
          );

      runWorker(worker, ignored1 -> {
        synchronized (runWorkersLock) {
          outputsReadyByLevel.computeIfAbsent(level, ignored2 -> new LongRBTreeSet())
                             .add(rank);
          superSorterProgressTracker.addMergedBatchesForLevel(level, 1);
        }
      });
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> void runWorker(final FrameProcessor<T> worker, final Consumer<T> outConsumer)
  {
    Futures.addCallback(
        exec.runFully(worker, cancellationId),
        new FutureCallback<T>()
        {
          @Override
          public void onSuccess(T result)
          {
            try {
              outConsumer.accept(result);

              synchronized (runWorkersLock) {
                workerFinished();
              }
            }
            catch (Throwable e) {
              synchronized (runWorkersLock) {
                allDone.setException(e);
              }
            }
          }

          @Override
          public void onFailure(Throwable t)
          {
            synchronized (runWorkersLock) {
              allDone.setException(t);
            }
          }
        },
        // Must run in exec, instead of in the same thread, to avoid running callback immediately if the
        // worker happens to finish super-quickly.
        exec.getExecutorService()
    );
  }

  // This also updates the progressTracker's number of total levels, and total mergers for levels. Therefore, if the
  // progressTracker is present, calling this multiple times will throw an error.
  @GuardedBy("runWorkersLock")
  private void setTotalInputFrames(final long totalInputFrames)
  {
    this.totalInputFrames = totalInputFrames;

    // Mark the progress tracker as trivially complete, if there is nothing to sort.
    if (totalInputFrames == 0) {
      superSorterProgressTracker.markTriviallyComplete();
    }

    // Set totalMergingLevels too
    long totalMergersInLevel = totalInputFrames;
    int level = 0;

    while (totalMergersInLevel > maxChannelsPerProcessor) {
      totalMergersInLevel = LongMath.divide(totalMergersInLevel, maxChannelsPerProcessor, RoundingMode.CEILING);
      superSorterProgressTracker.setTotalMergersForLevel(level, totalMergersInLevel);
      level++;
    }

    // Must have at least three levels. (Zero, penultimate, ultimate.)
    totalMergingLevels = Math.max(level + 1, 3);

    // Add remaining levels to the tracker, if required
    IntStream.range(level, totalMergingLevels)
             .forEach(curLevel -> {
               synchronized (runWorkersLock) {
                 superSorterProgressTracker.setTotalMergersForLevel(curLevel, 1);
               }
             });

    superSorterProgressTracker.setTotalMergingLevels(totalMergingLevels);
  }

  private ClusterByPartitions getOutputPartitions()
  {
    if (!outputPartitionsFuture.isDone()) {
      throw new ISE("Output partitions are not ready yet");
    }

    try {
      return outputPartitionsFuture.get();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
    catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @GuardedBy("runWorkersLock")
  private long getTotalMergersInLevel(final int level)
  {
    if (totalInputFrames == UNKNOWN_TOTAL || totalMergingLevels == UNKNOWN_LEVEL) {
      return UNKNOWN_TOTAL;
    } else if (level >= totalMergingLevels) {
      throw new ISE("Invalid level %d", level);
    } else if (level == totalMergingLevels - 1) {
      return outputPartitionsFuture.isDone() ? getOutputPartitions().size() : UNKNOWN_TOTAL;
    } else {
      long totalMergersInLevel = totalInputFrames;

      for (int i = 0; i <= level; i++) {
        totalMergersInLevel = LongMath.divide(totalMergersInLevel, maxChannelsPerProcessor, RoundingMode.CEILING);
      }

      return totalMergersInLevel;
    }
  }

  @GuardedBy("runWorkersLock")
  private boolean allInputRead()
  {
    return totalInputFrames != UNKNOWN_TOTAL;
  }

  /**
   * Whether this instance has finished its processing. This may be due to successful completion, or it may be due
   * to cancelation or error.
   *
   * Note: it is possible for this method to return true even when {@link #activeProcessors} is nonzero. Processors
   * take some time to exit after the instance becomes "done".
   */
  @GuardedBy("runWorkersLock")
  private boolean isAllDone()
  {
    return allDone.isDone() || allDone.isCancelled();
  }

  /**
   * Cleanup that must happen regardless of success or failure.
   */
  @GuardedBy("runWorkersLock")
  private void cleanUp()
  {
    if (!isAllDone() || activeProcessors != 0) {
      // This condition indicates a logic bug.
      throw new ISE("Improper cleanup");
    }

    if (log.isDebugEnabled()) {
      log.debug(stateString());
    }

    outputsReadyByLevel.clear();
    inputBuffer.clear();

    for (FrameFile frameFile : penultimateFrameFileCache.values()) {
      CloseableUtils.closeAndSuppressExceptions(
          frameFile,
          e -> log.warn(e, "Could not close intermediate file [%s]", frameFile.file())
      );
    }

    penultimateFrameFileCache.clear();

    if (!inputChannelsToRead.isEmpty()) {
      for (final ReadableFrameChannel inputChannel : inputChannels) {
        CloseableUtils.closeAndSuppressExceptions(
            inputChannel::doneReading,
            e -> log.warn(e, "Could not close input channel")
        );
      }

      inputChannels.forEach(ReadableFrameChannel::doneReading);
    }

    inputChannelsToRead.clear();
  }

  private File mergerOutputFile(final int level, final long rank)
  {
    return new File(directory, StringUtils.format("merged.%d.%d", level, rank));
  }

  /**
   * Returns a string encapsulating the current state of this object.
   */
  public String stateString()
  {
    synchronized (runWorkersLock) {
      return "frames-in=" + inputFramesReadSoFar + "/" + totalInputFrames
             + " frames-buffered=" + inputBuffer.size()
             + " lvls=" + totalMergingLevels
             + " parts=" +
             (outputPartitionsFuture.isDone() ? TalariaFutureUtils.getUncheckedImmediately(outputPartitionsFuture).size() : -1)
             + " p=" + activeProcessors + "/" + maxActiveProcessors
             + " ch-pending=" + inputChannelsToRead
             + " to-merge=" + outputsReadyByLevel
             + " done=" + (isAllDone() ? "y" : "n");
    }
  }

  private static ReadableFrameChannel singleReadableFrameChannel(final FrameWithPartition frame)
  {
    final BlockingQueueFrameChannel channel = BlockingQueueFrameChannel.minimal();
    channel.write(Try.value(frame));
    channel.doneWriting();
    return channel;
  }
}
