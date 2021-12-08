/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import io.imply.druid.talaria.frame.processor.FrameProcessorFactory;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.util.DimensionSchemaUtils;
import io.imply.druid.talaria.util.SupplierIterator;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class QueryWorkerUtils
{
  private QueryWorkerUtils()
  {
    // No instantiation.
  }

  /**
   * Given a {@link QueryWorkerInputSpec}, returns an iterator of windowed Segment objects that contain the data
   * that should be processed.
   *
   * Expected to be used by {@link FrameProcessorFactory} implementations.
   *
   * @param inputSpec           worker input specification
   * @param inputChannels       worker input channels; only used if inputSpec is type SUBQUERY
   * @param dataSegmentProvider fetcher of segments, used when {@link QueryWorkerInputSpec#getSegments()} is nonnull
   * @param temporaryDirectory  temporary directory used for fetching data from certain {@link InputSource}
   */
  public static Iterator<QueryWorkerInput> inputIterator(
      final QueryWorkerInputSpec inputSpec,
      final InputChannels inputChannels,
      final DataSegmentProvider dataSegmentProvider,
      final File temporaryDirectory
  )
  {
    if (inputSpec.type() == QueryWorkerInputType.SUBQUERY) {
      return Iterators.transform(
          Iterables.filter(
              inputChannels.getStagePartitions(),
              stagePartition -> inputSpec.getStageNumber() == stagePartition.getStageId().getStageNumber()
          ).iterator(),
          stagePartition -> {
            try {
              return QueryWorkerInput.forInputChannel(
                  inputChannels.openChannel(stagePartition),
                  inputChannels.getFrameReader(stagePartition)
              );
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
      );
    } else if (inputSpec.type() == QueryWorkerInputType.TABLE) {
      return Iterators.transform(
          dataSegmentIterator(inputSpec.getSegments(), dataSegmentProvider),
          QueryWorkerInput::forSegment
      );
    } else if (inputSpec.type() == QueryWorkerInputType.EXTERNAL) {
      return Iterators.transform(
          inputSourceSegmentIterator(
              inputSpec.getInputSource(),
              inputSpec.getInputFormat(),
              inputSpec.getSignature(),
              temporaryDirectory
          ),
          QueryWorkerInput::forSegment
      );
    } else {
      throw new UOE("Unsupported type [%s]", inputSpec.type());
    }
  }

  public static int numProcessors(@Nullable final QueryWorkerInputSpec inputSpec, final InputChannels inputChannels)
  {
    if (inputSpec == null) {
      return inputChannels.getStagePartitions().size();
    } else {
      return inputSpec.computeNumProcessors(inputChannels);
    }
  }

  private static Iterator<SegmentWithInterval> dataSegmentIterator(
      final List<DataSegmentWithInterval> descriptors,
      final DataSegmentProvider dataSegmentProvider
  )
  {
    return descriptors.stream().map(
        descriptor -> {
          final LazyResourceHolder<Segment> segmentHolder = dataSegmentProvider.fetchSegment(descriptor.getSegment());

          return new SegmentWithInterval(
              segmentHolder,
              descriptor.getSegment().getId(),
              descriptor.getQueryInterval()
          );
        }
    ).iterator();
  }

  private static Iterator<SegmentWithInterval> inputSourceSegmentIterator(
      final InputSource inputSource,
      final InputFormat inputFormat,
      final RowSignature signature,
      final File temporaryDirectory
  )
  {
    final InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("__dummy__", "auto", DateTimes.utc(0)),
        new DimensionsSpec(
            signature.getColumnNames().stream().map(
                column ->
                    DimensionSchemaUtils.createDimensionSchema(
                        column,
                        signature.getColumnType(column).map(ColumnType::getType).orElse(null)
                    )
            ).collect(Collectors.toList())
        ),
        ColumnsFilter.all()
    );

    if (!temporaryDirectory.exists() && !temporaryDirectory.mkdir()) {
      throw new ISE("Cannot create temporary directory at [%s]", temporaryDirectory);
    }

    final InputSourceReader reader = inputSource.reader(schema, inputFormat, temporaryDirectory);

    return new SupplierIterator<>(
        () -> {
          final SegmentId segmentId = SegmentId.dummy("dummy");
          final RowBasedSegment<InputRow> segment = new RowBasedSegment<>(
              segmentId,
              new BaseSequence<>(
                  new BaseSequence.IteratorMaker<InputRow, CloseableIterator<InputRow>>()
                  {
                    @Override
                    public CloseableIterator<InputRow> make()
                    {
                      try {
                        // TODO(gianm): parse-exception handling logic like that in FilteringCloseableInputRowIterator
                        // TODO(gianm): include location (file, lineno) information in parse exceptions
                        return reader.read();
                      }
                      catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }

                    @Override
                    public void cleanup(CloseableIterator<InputRow> iterFromMake)
                    {
                      try {
                        iterFromMake.close();
                      }
                      catch (IOException e) {
                        throw new RuntimeException(e);
                      }
                    }
                  }
              ),
              RowAdapters.standardRow(),
              signature
          );

          return new SegmentWithInterval(
              new LazyResourceHolder<>(() -> Pair.of(segment, () -> {})),
              segmentId,
              Intervals.ETERNITY
          );
        }
    );
  }
}
