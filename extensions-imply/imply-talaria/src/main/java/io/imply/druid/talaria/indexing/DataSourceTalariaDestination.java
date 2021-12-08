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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class DataSourceTalariaDestination implements TalariaDestination
{
  static final String TYPE = "dataSource";

  private final String dataSource;
  private final Granularity segmentGranularity;

  @Nullable
  private final List<Interval> replaceTimeChunks;

  @JsonCreator
  public DataSourceTalariaDestination(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      @JsonProperty("replaceTimeChunks") @Nullable List<Interval> replaceTimeChunks
  )
  {
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.segmentGranularity = Preconditions.checkNotNull(segmentGranularity, "segmentGranularity");
    this.replaceTimeChunks = replaceTimeChunks;

    if (replaceTimeChunks != null) {
      // Verify that if replaceTimeChunks is provided, it is nonempty.
      if (replaceTimeChunks.isEmpty()) {
        throw new IAE("replaceTimeChunks must be null or nonempty; cannot be empty");
      }

      // Verify all provided time chunks are aligned with segmentGranularity.
      for (final Interval interval : replaceTimeChunks) {
        // ETERNITY gets a free pass.
        if (!Intervals.ETERNITY.equals(interval)) {
          final boolean startIsAligned =
              segmentGranularity.bucketStart(interval.getStart()).equals(interval.getStart());

          final boolean endIsAligned =
              segmentGranularity.bucketStart(interval.getEnd()).equals(interval.getEnd())
              || segmentGranularity.increment(segmentGranularity.bucketStart(interval.getEnd()))
                                   .equals(interval.getEnd());

          if (!startIsAligned || !endIsAligned) {
            throw new IAE(
                "Time chunk [%s] provided in replaceTimeChunks is not aligned with segmentGranularity [%s]",
                interval,
                segmentGranularity
            );
          }
        }
      }
    }
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Granularity getSegmentGranularity()
  {
    return segmentGranularity;
  }

  /**
   * Returns the list of time chunks to replace, or null if {@link #isReplaceTimeChunks()} is false.
   *
   * Invariants: if nonnull, then this will *also* be nonempty, and all intervals will be aligned
   * with {@link #getSegmentGranularity()}. Each interval may comprise multiple time chunks.
   */
  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<Interval> getReplaceTimeChunks()
  {
    return replaceTimeChunks;
  }

  /**
   * Whether this object is in replace-existing-time-chunks mode.
   */
  public boolean isReplaceTimeChunks()
  {
    return replaceTimeChunks != null;
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
    DataSourceTalariaDestination that = (DataSourceTalariaDestination) o;
    return Objects.equals(dataSource, that.dataSource)
           && Objects.equals(segmentGranularity, that.segmentGranularity)
           && Objects.equals(replaceTimeChunks, that.replaceTimeChunks);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, segmentGranularity, replaceTimeChunks);
  }

  @Override
  public String toString()
  {
    return "DataSourceTalariaDestination{" +
           "dataSource='" + dataSource + '\'' +
           ", segmentGranularity=" + segmentGranularity +
           ", replaceTimeChunks=" + replaceTimeChunks +
           '}';
  }
}
