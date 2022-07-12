/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.google.common.collect.Iterators;
import io.imply.druid.talaria.counters.ChannelCounters;
import io.imply.druid.talaria.counters.CounterNames;
import io.imply.druid.talaria.counters.CounterTracker;
import io.imply.druid.talaria.querykit.DataSegmentProvider;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.SegmentId;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Reads slices of type {@link SegmentsInputSlice}.
 */
public class SegmentsInputSliceReader implements InputSliceReader
{
  private final DataSegmentProvider dataSegmentProvider;

  public SegmentsInputSliceReader(final DataSegmentProvider dataSegmentProvider)
  {
    this.dataSegmentProvider = dataSegmentProvider;
  }

  @Override
  public int numReadableInputs(InputSlice slice)
  {
    final SegmentsInputSlice segmentsInputSlice = (SegmentsInputSlice) slice;
    return segmentsInputSlice.getDescriptors().size();
  }

  @Override
  public ReadableInputs attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final SegmentsInputSlice segmentsInputSlice = (SegmentsInputSlice) slice;

    return ReadableInputs.segments(
        () -> Iterators.transform(
            dataSegmentIterator(
                segmentsInputSlice.getDataSource(),
                segmentsInputSlice.getDescriptors(),
                counters.channel(CounterNames.inputChannel(inputNumber)).setTotalFiles(slice.numFiles())
            ),
            ReadableInput::segment
        )
    );
  }

  private Iterator<SegmentWithDescriptor> dataSegmentIterator(
      final String dataSource,
      final List<RichSegmentDescriptor> descriptors,
      final ChannelCounters channelCounters
  )
  {
    return descriptors.stream().map(
        descriptor -> {
          final SegmentId segmentId = SegmentId.of(
              dataSource,
              descriptor.getFullInterval(),
              descriptor.getVersion(),
              descriptor.getPartitionNumber()
          );

          final ResourceHolder<Segment> segmentHolder = dataSegmentProvider.fetchSegment(segmentId, channelCounters);
          return new SegmentWithDescriptor(segmentHolder, descriptor);
        }
    ).iterator();
  }
}
