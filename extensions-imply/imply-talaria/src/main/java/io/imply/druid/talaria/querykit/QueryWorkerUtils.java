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
import io.imply.druid.talaria.indexing.CountableInputSourceReader;
import io.imply.druid.talaria.indexing.InputChannels;
import io.imply.druid.talaria.indexing.MSQCounterType;
import io.imply.druid.talaria.indexing.TalariaCounters;
import io.imply.druid.talaria.indexing.WarningCounters;
import io.imply.druid.talaria.indexing.error.CannotParseExternalDataFault;
import io.imply.druid.talaria.indexing.error.TalariaWarningReportPublisher;
import io.imply.druid.talaria.kernel.StageDefinition;
import io.imply.druid.talaria.util.DimensionSchemaUtils;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.Query;
import org.apache.druid.query.expression.TimestampFloorExprMacro;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * Worker-side utility methods.
 *
 * @see QueryKitUtils for controller-side utility methods
 */
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
   * @param workerNumber        worker number
   * @param inputSpec           worker input specification
   * @param stageDefinition     stage definition
   * @param inputChannels       worker input channels; only used if inputSpec is type SUBQUERY
   * @param dataSegmentProvider fetcher of segments, used when {@link QueryWorkerInputSpec#getSegments()} is nonnull
   * @param temporaryDirectory  temporary directory used for fetching data from certain {@link InputSource}
   */
  public static Iterator<QueryWorkerInput> inputIterator(
      final int workerNumber,
      final QueryWorkerInputSpec inputSpec,
      final StageDefinition stageDefinition,
      final InputChannels inputChannels,
      final DataSegmentProvider dataSegmentProvider,
      final File temporaryDirectory,
      final TalariaCounters talariaCounters,
      final TalariaWarningReportPublisher talariaWarningReportPublisher
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
          dataSegmentIterator(
              inputSpec.getSegments(),
              dataSegmentProvider,
              talariaCounters.getOrCreateChannelCounters(
                  MSQCounterType.INPUT_DRUID,
                  workerNumber,
                  stageDefinition.getStageNumber(),
                  -1
              )
          ),
          QueryWorkerInput::forSegment
      );
    } else if (inputSpec.type() == QueryWorkerInputType.EXTERNAL) {
      return Iterators.transform(
          inputSourceSegmentIterator(
              inputSpec.getInputSources(),
              inputSpec.getInputFormat(),
              inputSpec.getSignature(),
              temporaryDirectory,
              talariaCounters.getOrCreateChannelCounters(
                  MSQCounterType.INPUT_EXTERNAL,
                  workerNumber,
                  stageDefinition.getStageNumber(),
                  -1
              ),
              talariaCounters.getOrCreateWarningCounters(workerNumber, stageDefinition.getStageNumber()),
              talariaWarningReportPublisher,
              stageDefinition.getStageNumber()
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

  /**
   * Returns a virtual column named {@link QueryKitUtils#SEGMENT_GRANULARITY_COLUMN} that computes a segment
   * granularity based on a particular time column. Returns null if no virtual column is needed because the
   * granularity is {@link Granularities#ALL}. Throws an exception if the provided granularity is not supported.
   *
   * @throws IllegalArgumentException if the provided granularity is not supported
   */
  @Nullable
  public static VirtualColumn makeSegmentGranularityVirtualColumn(final Query<?> query)
  {
    final Granularity segmentGranularity = QueryKitUtils.getSegmentGranularityFromContext(query.getContext());
    final String timeColumnName = query.getContextValue(QueryKitUtils.CTX_TIME_COLUMN_NAME);

    if (timeColumnName == null || Granularities.ALL.equals(segmentGranularity)) {
      return null;
    }

    if (!(segmentGranularity instanceof PeriodGranularity)) {
      throw new IAE("Granularity [%s] is not supported", segmentGranularity);
    }

    final PeriodGranularity periodSegmentGranularity = (PeriodGranularity) segmentGranularity;

    if (periodSegmentGranularity.getOrigin() != null
        || !periodSegmentGranularity.getTimeZone().equals(DateTimeZone.UTC)) {
      throw new IAE("Granularity [%s] is not supported", segmentGranularity);
    }

    return new ExpressionVirtualColumn(
        QueryKitUtils.SEGMENT_GRANULARITY_COLUMN,
        StringUtils.format(
            "timestamp_floor(%s, %s)",
            CalciteSqlDialect.DEFAULT.quoteIdentifier(timeColumnName),
            Calcites.escapeStringLiteral((periodSegmentGranularity).getPeriod().toString())
        ),
        ColumnType.LONG,
        new ExprMacroTable(Collections.singletonList(new TimestampFloorExprMacro()))
    );
  }

  private static Iterator<SegmentWithInterval> dataSegmentIterator(
      final List<DataSegmentWithInterval> descriptors,
      final DataSegmentProvider dataSegmentProvider,
      final TalariaCounters.ChannelCounters channelCounters
  )
  {
    return descriptors.stream().map(
        descriptor -> {
          final LazyResourceHolder<Segment> segmentHolder = dataSegmentProvider.fetchSegment(
              descriptor.getSegment(),
              channelCounters
          );

          return new SegmentWithInterval(
              segmentHolder,
              descriptor.getSegment().getId(),
              descriptor.getQueryInterval()
          );
        }
    ).iterator();
  }

  private static Iterator<SegmentWithInterval> inputSourceSegmentIterator(
      final List<InputSource> inputSources,
      final InputFormat inputFormat,
      final RowSignature signature,
      final File temporaryDirectory,
      final TalariaCounters.ChannelCounters inputExternalCounter,
      final WarningCounters warningCounters,
      final TalariaWarningReportPublisher talariaWarningReportPublisher,
      final int stageNumber
  )
  {
    final InputRowSchema schema = new InputRowSchema(
        new TimestampSpec("__dummy__", "auto", DateTimes.utc(0)),
        new DimensionsSpec(
            signature.getColumnNames().stream().map(
                column ->
                    DimensionSchemaUtils.createDimensionSchema(
                        column,
                        signature.getColumnType(column).orElse(null)
                    )
            ).collect(Collectors.toList())
        ),
        ColumnsFilter.all()
    );

    if (!temporaryDirectory.exists() && !temporaryDirectory.mkdir()) {
      throw new ISE("Cannot create temporary directory at [%s]", temporaryDirectory);
    }
    return Iterators.transform(
        inputSources.iterator(),
        inputSource -> {
          final InputSourceReader reader;
          final boolean incrementCounters = isFileBasedInputSource(inputSource);

          if (incrementCounters) {
            reader = new CountableInputSourceReader(
                inputSource.reader(schema, inputFormat, temporaryDirectory),
                inputExternalCounter
            );
          } else {
            reader = inputSource.reader(schema, inputFormat, temporaryDirectory);
          }

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
                        CloseableIterator<InputRow> baseIterator = reader.read();
                        return new CloseableIterator<InputRow>()
                        {
                          private InputRow next = null;

                          @Override
                          public void close() throws IOException
                          {
                            baseIterator.close();
                          }

                          @Override
                          public boolean hasNext()
                          {
                            while (true) {
                              try {
                                while (next == null && baseIterator.hasNext()) {
                                  next = baseIterator.next();
                                }
                                break;
                              }
                              catch (ParseException e) {
                                warningCounters.incrementWarningCount(CannotParseExternalDataFault.CODE);
                                talariaWarningReportPublisher.publishException(stageNumber, e);
                              }
                            }
                            return next != null;
                          }

                          @Override
                          public InputRow next()
                          {
                            if (!hasNext()) {
                              throw new NoSuchElementException();
                            }
                            final InputRow row = next;
                            next = null;
                            return row;
                          }
                        };
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
                        // We increment the file count whenever the caller calls clean up. So we can double count here
                        // if the callers are not carefull.
                        // This logic only works because we are using FilePerSplitHintSpec. Each input source only
                        // has one file
                        if (incrementCounters) {
                          inputExternalCounter.incrementFileCount();
                        }
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
              new LazyResourceHolder<>(() -> Pair.of(segment, () -> {
              })),
              segmentId,
              Intervals.ETERNITY
          );
        }
    );
  }

  static boolean isFileBasedInputSource(final InputSource inputSource)
  {
    return !(inputSource instanceof NilInputSource) && !(inputSource instanceof InlineInputSource);
  }
}
