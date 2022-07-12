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
import io.imply.druid.talaria.counters.WarningCounters;
import io.imply.druid.talaria.indexing.CountableInputSourceReader;
import io.imply.druid.talaria.indexing.error.CannotParseExternalDataFault;
import io.imply.druid.talaria.querykit.LazyResourceHolder;
import io.imply.druid.talaria.util.DimensionSchemaUtils;
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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Reads {@link ExternalInputSlice} using {@link RowBasedSegment} backed by {@link InputSource#reader}.
 */
public class ExternalInputSliceReader implements InputSliceReader
{
  private final File temporaryDirectory;

  public ExternalInputSliceReader(final File temporaryDirectory)
  {
    this.temporaryDirectory = temporaryDirectory;
  }

  @Override
  public int numReadableInputs(InputSlice slice)
  {
    final ExternalInputSlice externalInputSlice = (ExternalInputSlice) slice;
    return externalInputSlice.getInputSources().size();
  }

  @Override
  public ReadableInputs attach(
      final int inputNumber,
      final InputSlice slice,
      final CounterTracker counters,
      final Consumer<Throwable> warningPublisher
  )
  {
    final ExternalInputSlice externalInputSlice = (ExternalInputSlice) slice;

    return ReadableInputs.segments(
        () -> Iterators.transform(
            inputSourceSegmentIterator(
                externalInputSlice.getInputSources(),
                externalInputSlice.getInputFormat(),
                externalInputSlice.getSignature(),
                temporaryDirectory,
                counters.channel(CounterNames.inputChannel(inputNumber)).setTotalFiles(slice.numFiles()),
                counters.warnings(),
                warningPublisher
            ),
            ReadableInput::segment
        )
    );
  }

  private static Iterator<SegmentWithDescriptor> inputSourceSegmentIterator(
      final List<InputSource> inputSources,
      final InputFormat inputFormat,
      final RowSignature signature,
      final File temporaryDirectory,
      final ChannelCounters channelCounters,
      final WarningCounters warningCounters,
      final Consumer<Throwable> warningPublisher
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
                channelCounters
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
                                warningPublisher.accept(e);
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
                        // if the callers are not careful.
                        // This logic only works because we are using FilePerSplitHintSpec. Each input source only
                        // has one file.
                        if (incrementCounters) {
                          channelCounters.incrementFileCount();
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

          return new SegmentWithDescriptor(
              new LazyResourceHolder<>(() -> Pair.of(segment, () -> {})),
              segmentId.toDescriptor()
          );
        }
    );
  }

  static boolean isFileBasedInputSource(final InputSource inputSource)
  {
    return !(inputSource instanceof NilInputSource) && !(inputSource instanceof InlineInputSource);
  }
}
