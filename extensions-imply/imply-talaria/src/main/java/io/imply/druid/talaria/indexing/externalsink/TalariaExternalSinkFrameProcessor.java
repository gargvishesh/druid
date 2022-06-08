/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.externalsink;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.channel.WritableFrameChannel;
import io.imply.druid.talaria.frame.processor.FrameProcessor;
import io.imply.druid.talaria.frame.processor.FrameProcessors;
import io.imply.druid.talaria.frame.processor.ReturnOrAwait;
import io.imply.druid.talaria.frame.read.FrameReader;
import io.imply.druid.talaria.frame.segment.FrameStorageAdapter;
import io.imply.druid.talaria.indexing.ColumnMapping;
import io.imply.druid.talaria.indexing.ColumnMappings;
import io.imply.druid.talaria.util.SequenceUtils;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.VirtualColumns;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO(gianm): Move SQL ResultFormat into a package that enables using it here.
 * TODO(gianm): The format implemented here is like objectLines, but without the blank line at the end.
 * TODO(gianm): The blank line isn't necessary because talaria jobs have out-of-band ways to report success.
 */
public class TalariaExternalSinkFrameProcessor implements FrameProcessor<URI>
{
  private static final Logger log = new Logger(TalariaExternalSinkFrameProcessor.class);

  private final ReadableFrameChannel inChannel;
  private final FrameReader frameReader;
  private final ColumnMappings columnMappings;
  private final TalariaExternalSinkStream externalSinkStream;
  private final JsonGenerator jsonGenerator;

  private final SerializerProvider serializers;

  TalariaExternalSinkFrameProcessor(
      final ReadableFrameChannel inChannel,
      final FrameReader frameReader,
      final ColumnMappings columnMappings,
      final TalariaExternalSinkStream externalSinkStream,
      final ObjectMapper jsonMapper
  ) throws IOException
  {
    this.inChannel = inChannel;
    this.frameReader = frameReader;
    this.columnMappings = columnMappings;
    this.externalSinkStream = externalSinkStream;
    this.jsonGenerator = jsonMapper.writer().getFactory().createGenerator(externalSinkStream.getOutputStream());
    this.jsonGenerator.setRootValueSeparator(new SerializedString("\n"));
    this.serializers = jsonMapper.getSerializerProviderInstance();

    log.debug("Opened external sink file [%s].", externalSinkStream.getUri());
  }

  @Override
  public List<ReadableFrameChannel> inputChannels()
  {
    return Collections.singletonList(inChannel);
  }

  @Override
  public List<WritableFrameChannel> outputChannels()
  {
    return Collections.emptyList();
  }

  @Override
  public ReturnOrAwait<URI> runIncrementally(IntSet readableInputs) throws IOException
  {
    if (readableInputs.isEmpty()) {
      return ReturnOrAwait.awaitAll(1);
    }

    if (inChannel.isFinished()) {
      log.debug("Done writing to external sink file [%s].", externalSinkStream.getUri());

      jsonGenerator.flush();
      externalSinkStream.getOutputStream().write('\n');
      return ReturnOrAwait.returnObject(externalSinkStream.getUri());
    } else {
      writeFrame(inChannel.read().getOrThrow());
      return ReturnOrAwait.awaitAll(1);
    }
  }

  @Override
  public void cleanup() throws IOException
  {
    FrameProcessors.closeAll(
        inputChannels(),
        outputChannels(),
        jsonGenerator,
        externalSinkStream.getOutputStream()
    );
  }

  private void writeFrame(final Frame frame)
  {
    final Sequence<Cursor> cursorSequence =
        new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY)
            .makeCursors(null, Intervals.ETERNITY, VirtualColumns.EMPTY, Granularities.ALL, false, null);

    SequenceUtils.forEach(
        cursorSequence,
        cursor -> {
          final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();

          //noinspection rawtypes
          @SuppressWarnings("rawtypes")
          final List<BaseObjectColumnValueSelector> selectors =
              columnMappings.getMappings()
                            .stream()
                            .map(ColumnMapping::getQueryColumn)
                            .map(columnSelectorFactory::makeColumnValueSelector)
                            .collect(Collectors.toList());

          while (!cursor.isDone()) {
            try {
              writeRow(selectors);
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }

            cursor.advance();
          }
        }
    );
  }

  private void writeRow(
      @SuppressWarnings("rawtypes") final List<BaseObjectColumnValueSelector> selectors
  ) throws IOException
  {
    jsonGenerator.writeStartObject();

    for (int i = 0; i < selectors.size(); i++) {
      jsonGenerator.writeFieldName(columnMappings.getMappings().get(i).getOutputColumn());
      JacksonUtils.writeObjectUsingSerializerProvider(jsonGenerator, serializers, selectors.get(i).getObject());
    }

    jsonGenerator.writeEndObject();
  }
}
