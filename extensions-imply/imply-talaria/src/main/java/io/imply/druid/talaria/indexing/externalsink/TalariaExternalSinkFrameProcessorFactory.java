/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.externalsink;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.processor.FrameContext;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.frame.processor.OutputChannelFactory;
import io.imply.druid.talaria.frame.processor.OutputChannels;
import io.imply.druid.talaria.frame.processor.ProcessorsAndChannels;
import io.imply.druid.talaria.indexing.ColumnMappings;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.indexing.error.TalariaWarningReportPublisher;
import io.imply.druid.talaria.kernel.ExtraInfoHolder;
import io.imply.druid.talaria.kernel.NilExtraInfoHolder;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.kernel.StagePartition;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.IntStream;

@JsonTypeName("externalSink")
public class TalariaExternalSinkFrameProcessorFactory
    implements FrameProcessorFactory<Object, TalariaExternalSinkFrameProcessor, URI, SortedSet<URI>>
{
  private final ColumnMappings columnMappings;

  @JsonCreator
  public TalariaExternalSinkFrameProcessorFactory(@JsonProperty("columnMappings") final ColumnMappings columnMappings)
  {
    this.columnMappings = Preconditions.checkNotNull(columnMappings, "columnMappings");
  }

  @JsonProperty
  public ColumnMappings getColumnMappings()
  {
    return columnMappings;
  }

  @Override
  public ProcessorsAndChannels<TalariaExternalSinkFrameProcessor, URI> makeProcessors(
      final int workerNumber,
      final Object extraInfoIgnored,
      final InputChannels inputChannels,
      final OutputChannelFactory outputChannelFactory,
      final StageDefinition stageDefinition,
      final ClusterBy clusterBy,
      final FrameContext providerThingy,
      final int maxOutstandingProcessors,
      final TalariaCounters talariaCounters,
      @Nullable TalariaWarningReportPublisher talariaWarningReportPublisher
  )
  {
    final TalariaExternalSink externalSink = providerThingy.externalSink();
    final ObjectMapper jsonMapper = providerThingy.jsonMapper();

    final Sequence<Integer> inputSequence =
        Sequences.simple(() -> IntStream.range(0, inputChannels.getStagePartitions().size()).iterator());

    final Sequence<TalariaExternalSinkFrameProcessor> workers = inputSequence.map(
        i -> {
          final StagePartition stagePartition = inputChannels.getStagePartitions().get(i);

          try {
            final TalariaExternalSinkStream externalSinkStream = externalSink.open(
                stagePartition.getStageId().getQueryId(),
                stagePartition.getPartitionNumber()
            );

            final ReadableFrameChannel channel = inputChannels.openChannel(stagePartition);

            return new TalariaExternalSinkFrameProcessor(
                channel,
                inputChannels.getFrameReader(stagePartition),
                columnMappings,
                externalSinkStream,
                jsonMapper
            );
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    );

    return new ProcessorsAndChannels<>(workers, OutputChannels.none());
  }

  @Override
  public TypeReference<SortedSet<URI>> getAccumulatedResultTypeReference()
  {
    return new TypeReference<SortedSet<URI>>() {};
  }

  @Override
  public SortedSet<URI> newAccumulatedResult()
  {
    return new TreeSet<>();
  }

  @Nullable
  @Override
  public SortedSet<URI> accumulateResult(SortedSet<URI> accumulated, URI current)
  {
    accumulated.add(current);
    return accumulated;
  }

  @Nullable
  @Override
  public SortedSet<URI> mergeAccumulatedResult(SortedSet<URI> accumulated, SortedSet<URI> otherAccumulated)
  {
    accumulated.addAll(otherAccumulated);
    return accumulated;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public ExtraInfoHolder makeExtraInfoHolder(final Object extra)
  {
    if (extra != null) {
      throw new ISE("Expected null 'extra'");
    }

    return NilExtraInfoHolder.instance();
  }
}
