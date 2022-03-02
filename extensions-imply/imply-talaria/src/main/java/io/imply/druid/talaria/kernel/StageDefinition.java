/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.talaria.frame.cluster.ClusterByPartitions;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollector;
import io.imply.druid.talaria.frame.cluster.statistics.ClusterByStatisticsCollectorImpl;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.frame.read.FrameReader;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public class StageDefinition
{
  private static final int PARTITION_STATS_MAX_KEYS = 2 << 15; // Avoid immediate downsample of single-bucket collectors
  private static final int PARTITION_STATS_MAX_BUCKETS = 5_000; // Limit for TooManyBuckets
  private static final int MAX_PARTITIONS = 25_000; // Limit for TooManyPartitions

  private final StageId id;
  private final List<StageId> inputStages;
  private final Set<StageId> broadcastInputStages;
  @SuppressWarnings("rawtypes")
  private final FrameProcessorFactory processorFactory;
  private final RowSignature signature;
  private final int maxWorkerCount;

  @Nullable
  private final ShuffleSpec shuffleSpec;

  // Set here to encourage sharing, rather than re-creation.
  // TODO(gianm): maybe not the greatest place to put this, but it's expedient. evaluate moving this somewhere else.
  private final Supplier<FrameReader> frameReader;

  @JsonCreator
  StageDefinition(
      @JsonProperty("id") final StageId id,
      @JsonProperty("inputStages") final List<StageId> inputStages,
      @JsonProperty("broadcastInputStages") final Set<StageId> broadcastInputStages,
      @SuppressWarnings("rawtypes") @JsonProperty("processorFactory") final FrameProcessorFactory processorFactory,
      @JsonProperty("signature") final RowSignature signature,
      @Nullable @JsonProperty("shuffleSpec") final ShuffleSpec shuffleSpec,
      @JsonProperty("maxWorkerCount") final int maxWorkerCount
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.inputStages = Preconditions.checkNotNull(inputStages, "inputStages");
    this.broadcastInputStages = broadcastInputStages != null ? broadcastInputStages : Collections.emptySet();
    this.processorFactory = Preconditions.checkNotNull(processorFactory, "processorFactory");
    this.signature = Preconditions.checkNotNull(signature, "signature");
    this.shuffleSpec = shuffleSpec;
    this.maxWorkerCount = maxWorkerCount;
    this.frameReader = Suppliers.memoize(() -> FrameReader.create(signature))::get;

    if (ImmutableSet.copyOf(inputStages).size() != inputStages.size()) {
      throw new IAE("Cannot accept duplicate input stages");
    }

    if (!inputStages.stream().map(StageId::getQueryId).allMatch(queryId -> queryId.equals(id.getQueryId()))) {
      throw new IAE("Cannot accept stages with query ids that do not match our own");
    }

    if (shuffleSpec != null && shuffleSpec.needsStatistics() && shuffleSpec.getClusterBy().getColumns().isEmpty()) {
      throw new IAE("Cannot shuffle with spec [%s] and nil clusterBy", shuffleSpec);
    }

    for (final String columnName : signature.getColumnNames()) {
      if (!signature.getColumnType(columnName).isPresent()) {
        throw new ISE("Missing type for column [%s]", columnName);
      }
    }

    for (final StageId broadcastInputStage : this.broadcastInputStages) {
      if (!inputStages.contains(broadcastInputStage)) {
        throw new ISE("Cannot accept broadcast data from non-input stage [%s]", broadcastInputStage);
      }
    }
  }

  public static StageDefinitionBuilder builder(final int stageNumber)
  {
    return new StageDefinitionBuilder(stageNumber);
  }

  public static StageDefinitionBuilder builder(final StageDefinition stageDef)
  {
    return new StageDefinitionBuilder(stageDef.getStageNumber())
        .inputStages(stageDef.getInputStageIds().stream().mapToInt(StageId::getStageNumber).toArray())
        .broadcastInputStages(stageDef.getBroadcastInputStageIds().stream().mapToInt(StageId::getStageNumber).toArray())
        .processorFactory(stageDef.getProcessorFactory())
        .signature(stageDef.getSignature())
        .shuffleSpec(stageDef.getShuffleSpec().orElse(null))
        .maxWorkerCount(stageDef.getMaxWorkerCount());
  }

  @JsonProperty
  public StageId getId()
  {
    return id;
  }

  @JsonProperty("inputStages")
  public List<StageId> getInputStageIds()
  {
    return inputStages;
  }

  @JsonProperty("broadcastInputStages")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Set<StageId> getBroadcastInputStageIds()
  {
    return broadcastInputStages;
  }

  @JsonProperty
  @SuppressWarnings("rawtypes")
  public FrameProcessorFactory getProcessorFactory()
  {
    return processorFactory;
  }

  @JsonProperty
  public RowSignature getSignature()
  {
    return signature;
  }

  public boolean doesShuffle()
  {
    return shuffleSpec != null;
  }

  public Optional<ShuffleSpec> getShuffleSpec()
  {
    return Optional.ofNullable(shuffleSpec);
  }

  @Nullable
  @JsonProperty("shuffleSpec")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ShuffleSpec getShuffleSpecForSerialization()
  {
    return shuffleSpec;
  }

  @JsonProperty
  public int getMaxWorkerCount()
  {
    return maxWorkerCount;
  }

  public int getMaxPartitionCount()
  {
    // Pretends to be an instance method, but really returns a constant. Maybe one day this will be defined per stage.
    return MAX_PARTITIONS;
  }

  public int getStageNumber()
  {
    return id.getStageNumber();
  }

  public boolean mustGatherResultKeyStatistics()
  {
    return shuffleSpec != null && shuffleSpec.needsStatistics();
  }

  public Either<Long, ClusterByPartitions> generatePartitionsForShuffle(
      @Nullable ClusterByStatisticsCollector collector
  )
  {
    if (shuffleSpec == null) {
      throw new ISE("No shuffle");
    } else if (mustGatherResultKeyStatistics() && collector == null) {
      throw new ISE("Statistics required, but not gathered");
    } else if (!mustGatherResultKeyStatistics() && collector != null) {
      throw new ISE("Statistics gathered, but not required");
    } else {
      return shuffleSpec.generatePartitions(collector, MAX_PARTITIONS);
    }
  }

  public ClusterByStatisticsCollector createResultKeyStatisticsCollector()
  {
    if (!mustGatherResultKeyStatistics()) {
      throw new ISE("No statistics needed");
    }

    return ClusterByStatisticsCollectorImpl.create(
        shuffleSpec.getClusterBy(),
        signature,
        PARTITION_STATS_MAX_KEYS,
        PARTITION_STATS_MAX_BUCKETS,
        shuffleSpec.doesAggregateByClusterKey()
    );
  }

  public FrameReader getFrameReader()
  {
    return frameReader.get();
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
    StageDefinition that = (StageDefinition) o;
    return maxWorkerCount == that.maxWorkerCount
           && Objects.equals(id, that.id)
           && Objects.equals(inputStages, that.inputStages)
           && Objects.equals(broadcastInputStages, that.broadcastInputStages)
           && Objects.equals(processorFactory, that.processorFactory)
           && Objects.equals(signature, that.signature)
           && Objects.equals(shuffleSpec, that.shuffleSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        id,
        inputStages,
        broadcastInputStages,
        processorFactory,
        signature,
        maxWorkerCount,
        shuffleSpec
    );
  }

  @Override
  public String toString()
  {
    return "StageDefinition{" +
           "id=" + id +
           ", inputStages=" + inputStages +
           (!broadcastInputStages.isEmpty() ? ", broadcastInputStages=" + broadcastInputStages : "") +
           ", processorFactory=" + processorFactory +
           ", signature=" + signature +
           ", maxWorkerCount=" + maxWorkerCount +
           ", shuffleSpec=" + shuffleSpec +
           '}';
  }
}
