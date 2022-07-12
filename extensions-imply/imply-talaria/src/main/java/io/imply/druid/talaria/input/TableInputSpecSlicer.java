/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.google.common.base.Preconditions;
import com.google.common.math.LongMath;
import com.google.common.primitives.Ints;
import io.imply.druid.talaria.exec.Limits;
import io.imply.druid.talaria.kernel.SplitUtils;
import io.imply.druid.talaria.querykit.DataSegmentTimelineView;
import org.apache.druid.query.filter.DimFilterUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.joda.time.Interval;

import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * Slices {@link TableInputSpec} into {@link SegmentsInputSlice}.
 */
public class TableInputSpecSlicer implements InputSpecSlicer
{
  private final DataSegmentTimelineView timelineView;

  public TableInputSpecSlicer(DataSegmentTimelineView timelineView)
  {
    this.timelineView = timelineView;
  }

  @Override
  public boolean canSliceDynamic(InputSpec inputSpec)
  {
    return true;
  }

  @Override
  public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
  {
    final TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;
    final Set<DataSegmentWithInterval> prunedSegmentSet = getPrunedSegmentSet(tableInputSpec);
    return makeSlices(tableInputSpec, prunedSegmentSet, maxNumSlices);
  }

  @Override
  public List<InputSlice> sliceDynamic(
      InputSpec inputSpec,
      int maxNumSlices,
      int maxFilesPerSlice,
      long maxBytesPerSlice
  )
  {
    final TableInputSpec tableInputSpec = (TableInputSpec) inputSpec;
    final Set<DataSegmentWithInterval> prunedSegmentSet = getPrunedSegmentSet(tableInputSpec);

    long totalSizeInBytes = 0;

    for (DataSegmentWithInterval segmentWithInterval : prunedSegmentSet) {
      totalSizeInBytes += segmentWithInterval.getSegment().getSize();
    }

    final int numSlices =
        Ints.checkedCast(LongMath.divide(totalSizeInBytes, Limits.MAX_INPUT_BYTES_PER_WORKER, RoundingMode.CEILING));

    return makeSlices(tableInputSpec, prunedSegmentSet, Math.min(maxNumSlices, numSlices));
  }

  private Set<DataSegmentWithInterval> getPrunedSegmentSet(final TableInputSpec tableInputSpec)
  {
    final TimelineLookup<String, DataSegment> timeline =
        timelineView.getTimeline(tableInputSpec.getDataSource(), tableInputSpec.getIntervals()).orElse(null);

    if (timeline == null) {
      return Collections.emptySet();
    } else {
      final Iterator<DataSegmentWithInterval> dataSegmentIterator =
          tableInputSpec.getIntervals().stream()
                        .flatMap(interval -> timeline.lookup(interval).stream())
                        .flatMap(
                            holder ->
                                StreamSupport.stream(holder.getObject().spliterator(), false)
                                             .map(
                                                 chunk ->
                                                     new DataSegmentWithInterval(
                                                         chunk.getObject(),
                                                         holder.getInterval()
                                                     )
                                             )
                        ).iterator();

      return DimFilterUtils.filterShards(
          tableInputSpec.getFilter(),
          () -> dataSegmentIterator,
          segment -> segment.getSegment().getShardSpec()
      );
    }
  }

  private List<InputSlice> makeSlices(
      final TableInputSpec tableInputSpec,
      final Set<DataSegmentWithInterval> prunedSegmentSet,
      final int maxNumSlices
  )
  {
    if (prunedSegmentSet.isEmpty()) {
      return Collections.emptyList();
    }

    final List<List<DataSegmentWithInterval>> assignments = SplitUtils.makeSplits(
        prunedSegmentSet.iterator(),
        segment -> segment.getSegment().getSize(),
        maxNumSlices
    );

    final List<InputSlice> retVal = new ArrayList<>();

    for (final List<DataSegmentWithInterval> dataSegmentWithIntervals : assignments) {
      final List<RichSegmentDescriptor> descriptors = new ArrayList<>();
      for (final DataSegmentWithInterval dataSegmentWithInterval : dataSegmentWithIntervals) {
        descriptors.add(dataSegmentWithInterval.toRichSegmentDescriptor());
      }

      if (descriptors.isEmpty()) {
        retVal.add(NilInputSlice.INSTANCE);
      } else {
        retVal.add(new SegmentsInputSlice(tableInputSpec.getDataSource(), descriptors));
      }
    }

    return retVal;
  }

  private static class DataSegmentWithInterval
  {
    private final DataSegment segment;
    private final Interval interval;

    public DataSegmentWithInterval(DataSegment segment, Interval interval)
    {
      this.segment = Preconditions.checkNotNull(segment, "segment");
      this.interval = Preconditions.checkNotNull(interval, "interval");
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public RichSegmentDescriptor toRichSegmentDescriptor()
    {
      return new RichSegmentDescriptor(
          segment.getInterval(),
          interval,
          segment.getVersion(),
          segment.getShardSpec().getPartitionNum()
      );
    }
  }
}
