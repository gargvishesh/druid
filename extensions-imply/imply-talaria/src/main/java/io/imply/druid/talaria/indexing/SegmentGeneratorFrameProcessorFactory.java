/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import io.imply.druid.talaria.counters.CounterTracker;
import io.imply.druid.talaria.exec.WorkerMemoryParameters;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSliceReader;
import io.imply.druid.talaria.input.ReadableInput;
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.FrameProcessorFactory;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StagePartition;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

@JsonTypeName("segmentGenerator")
public class SegmentGeneratorFrameProcessorFactory
    implements FrameProcessorFactory<List<SegmentIdWithShardSpec>, SegmentGeneratorFrameProcessor, DataSegment, Set<DataSegment>>
{
  private final DataSchema dataSchema;
  private final ColumnMappings columnMappings;
  private final ParallelIndexTuningConfig tuningConfig;

  @JsonCreator
  public SegmentGeneratorFrameProcessorFactory(
      @JsonProperty("dataSchema") final DataSchema dataSchema,
      @JsonProperty("columnMappings") final ColumnMappings columnMappings,
      @JsonProperty("tuningConfig") final ParallelIndexTuningConfig tuningConfig
  )
  {
    this.dataSchema = Preconditions.checkNotNull(dataSchema, "dataSchema");
    this.columnMappings = Preconditions.checkNotNull(columnMappings, "columnMappings");
    this.tuningConfig = Preconditions.checkNotNull(tuningConfig, "tuningConfig");
  }

  @JsonProperty
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty
  public ColumnMappings getColumnMappings()
  {
    return columnMappings;
  }

  @JsonProperty
  public ParallelIndexTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  @Override
  public ProcessorsAndChannels<SegmentGeneratorFrameProcessor, DataSegment> makeProcessors(
      StageDefinition stageDefinition,
      int workerNumber,
      List<InputSlice> inputSlices,
      InputSliceReader inputSliceReader,
      @Nullable List<SegmentIdWithShardSpec> extra,
      OutputChannelFactory outputChannelFactory,
      FrameContext frameContext,
      int maxOutstandingProcessors,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  )
  {
    final RowIngestionMeters meters = frameContext.rowIngestionMeters();

    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        meters,
        tuningConfig.isLogParseExceptions(),
        tuningConfig.getMaxParseExceptions(),
        tuningConfig.getMaxSavedParseExceptions()
    );

    // Expect a single input slice.
    final InputSlice slice = Iterables.getOnlyElement(inputSlices);
    final Sequence<Pair<Integer, ReadableInput>> inputSequence =
        Sequences.simple(Iterables.transform(
            inputSliceReader.attach(0, slice, counters, warningPublisher),
            new Function<ReadableInput, Pair<Integer, ReadableInput>>()
            {
              int i = 0;

              @Override
              public Pair<Integer, ReadableInput> apply(ReadableInput readableInput)
              {
                return Pair.of(i++, readableInput);
              }
            }
        ));

    final Sequence<SegmentGeneratorFrameProcessor> workers = inputSequence.map(
        readableInputPair -> {
          final StagePartition stagePartition = Preconditions.checkNotNull(readableInputPair.rhs.getStagePartition());
          final SegmentIdWithShardSpec segmentIdWithShardSpec = extra.get(readableInputPair.lhs);
          final String idString = StringUtils.format("%s:%s", stagePartition, workerNumber);
          final File persistDirectory = new File(
              frameContext.persistDir(),
              segmentIdWithShardSpec.asSegmentId().toString()
          );

          // Create directly, without using AppenderatorsManager, because we need different memory overrides due to
          // using one Appenderator per processing thread instead of per task.
          // Note: "createOffline" ignores the batchProcessingMode and always acts like CLOSED_SEGMENTS_SINKS.
          final Appenderator appenderator =
              Appenderators.createOffline(
                  idString,
                  dataSchema,
                  makeAppenderatorConfig(
                      tuningConfig,
                      persistDirectory,
                      frameContext.memoryParameters()
                  ),
                  new FireDepartmentMetrics(), // We should eventually expose the FireDepartmentMetrics
                  frameContext.segmentPusher(),
                  frameContext.jsonMapper(),
                  frameContext.indexIO(),
                  frameContext.indexMerger(),
                  meters,
                  parseExceptionHandler,
                  true
              );

          return new SegmentGeneratorFrameProcessor(
              readableInputPair.rhs,
              columnMappings,
              dataSchema.getDimensionsSpec().getDimensionNames(),
              appenderator,
              segmentIdWithShardSpec
          );
        }
    );

    return new ProcessorsAndChannels<>(workers, OutputChannels.none());
  }

  @Override
  public TypeReference<Set<DataSegment>> getAccumulatedResultTypeReference()
  {
    return new TypeReference<Set<DataSegment>>() {};
  }

  @Override
  public Set<DataSegment> newAccumulatedResult()
  {
    return new HashSet<>();
  }

  @Nullable
  @Override
  public Set<DataSegment> accumulateResult(Set<DataSegment> accumulated, DataSegment current)
  {
    if (current != null) {
      accumulated.add(current);
    }

    return accumulated;
  }

  @Nullable
  @Override
  public Set<DataSegment> mergeAccumulatedResult(Set<DataSegment> accumulated, Set<DataSegment> otherAccumulated)
  {
    accumulated.addAll(otherAccumulated);
    return accumulated;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentGeneratorFrameProcessorFactory that = (SegmentGeneratorFrameProcessorFactory) o;
    return Objects.equals(dataSchema, that.dataSchema)
           && Objects.equals(columnMappings, that.columnMappings)
           && Objects.equals(tuningConfig, that.tuningConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSchema, columnMappings, tuningConfig);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExtraInfoHolder makeExtraInfoHolder(final List<SegmentIdWithShardSpec> segmentIdsWithShardSpecs)
  {
    return new SegmentGeneratorExtraInfoHolder(segmentIdsWithShardSpecs);
  }

  private static AppenderatorConfig makeAppenderatorConfig(
      final ParallelIndexTuningConfig tuningConfig,
      final File persistDirectory,
      final WorkerMemoryParameters memoryParameters
  )
  {
    return new AppenderatorConfig()
    {
      @Override
      public AppendableIndexSpec getAppendableIndexSpec()
      {
        return tuningConfig.getAppendableIndexSpec();
      }

      @Override
      public int getMaxRowsInMemory()
      {
        // No need to apportion this amongst memoryParameters.getAppenderatorCount(), because it only exists
        // to minimize the impact of per-row overheads that are not accounted for by OnheapIncrementalIndex's
        // maxBytesInMemory handling. For example: overheads related to creating bitmaps during persist.
        return tuningConfig.getMaxRowsInMemory();
      }

      @Override
      public long getMaxBytesInMemory()
      {
        return memoryParameters.getAppenderatorMaxBytesInMemory();
      }

      @Override
      public PartitionsSpec getPartitionsSpec()
      {
        return tuningConfig.getPartitionsSpec();
      }

      @Override
      public IndexSpec getIndexSpec()
      {
        return tuningConfig.getIndexSpec();
      }

      @Override
      public IndexSpec getIndexSpecForIntermediatePersists()
      {
        // Disable compression for intermediate persists to reduce direct memory usage.
        return new IndexSpec(
            null,
            CompressionStrategy.UNCOMPRESSED, // Dimensions don't support NONE, so use UNCOMPRESSED
            CompressionStrategy.NONE, // NONE is more efficient than UNCOMPRESSED
            CompressionFactory.LongEncodingStrategy.LONGS,
            null
        );
      }

      @Override
      public boolean isReportParseExceptions()
      {
        return tuningConfig.isReportParseExceptions();
      }

      @Override
      public int getMaxPendingPersists()
      {
        return tuningConfig.getMaxPendingPersists();
      }

      @Override
      public boolean isSkipBytesInMemoryOverheadCheck()
      {
        return tuningConfig.isSkipBytesInMemoryOverheadCheck();
      }

      @Override
      public Period getIntermediatePersistPeriod()
      {
        return tuningConfig.getIntermediatePersistPeriod();
      }

      @Override
      public File getBasePersistDirectory()
      {
        return persistDirectory;
      }

      @Override
      public AppenderatorConfig withBasePersistDirectory(File basePersistDirectory)
      {
        // Not used.
        throw new UnsupportedOperationException();
      }

      @Nullable
      @Override
      public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
      {
        return tuningConfig.getSegmentWriteOutMediumFactory();
      }

      @Override
      public int getMaxColumnsToMerge()
      {
        return memoryParameters.getAppenderatorMaxColumnsToMerge();
      }
    };
  }

  @JsonTypeName("segmentGenerator")
  public static class SegmentGeneratorExtraInfoHolder extends ExtraInfoHolder<List<SegmentIdWithShardSpec>>
  {
    @JsonCreator
    public SegmentGeneratorExtraInfoHolder(@Nullable @JsonProperty(INFO_KEY) final List<SegmentIdWithShardSpec> extra)
    {
      super(extra);
    }
  }
}
