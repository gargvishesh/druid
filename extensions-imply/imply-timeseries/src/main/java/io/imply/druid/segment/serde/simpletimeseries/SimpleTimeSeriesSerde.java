/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.base.Preconditions;
import io.imply.druid.timeseries.SimpleTimeSeries;
import io.imply.druid.timeseries.utils.ImplyDoubleArrayList;
import io.imply.druid.timeseries.utils.ImplyLongArrayList;
import org.apache.druid.segment.serde.cell.StagedSerde;
import org.apache.druid.segment.serde.cell.StorableBuffer;

import java.nio.ByteBuffer;

public class SimpleTimeSeriesSerde implements StagedSerde<SimpleTimeSeries>
{
  private static final int SINGLE_ITEM_LIST_SIZE_BYTES = Long.BYTES + Double.BYTES;
  private static final byte EXPECTED_ROW_HEADER_VERSION = 0;

  private final CellHeader.Reader headerReader = new CellHeader.Reader();
  private final CellHeader.Writer headerWriter = new CellHeader.Writer();
  private final TimestampsEncoderDecoder timeStampsEncoderDecoder;

  public SimpleTimeSeriesSerde(TimestampsEncoderDecoder timeStampsEncoderDecoder)
  {
    this.timeStampsEncoderDecoder = timeStampsEncoderDecoder;
  }

  public static SimpleTimeSeriesSerde create(long minTimestamp, boolean useInteger)
  {
    SimpleTimeSeriesSerde timeSeriesSerde;

    if (useInteger) {
      timeSeriesSerde = new SimpleTimeSeriesSerde(
          new IntegerDeltaTimestampsEncoderDecoder(new IntegerDeltaEncoderDecoder(minTimestamp)));
    } else {
      timeSeriesSerde =
          new SimpleTimeSeriesSerde(new LongDeltaTimestampsEncoderDecoder(new LongDeltaEncoderDecoder(minTimestamp)));
    }

    return timeSeriesSerde;
  }

  @Override
  public StorableBuffer serializeDelayed(SimpleTimeSeries simpleTimeSeries)
  {
    if (simpleTimeSeries == null) {
      return StorableBuffer.EMPTY;
    }

    // force flattening of recursive structures
    simpleTimeSeries.computeSimple();

    ImplyLongArrayList timestamps = simpleTimeSeries.getTimestamps();
    ImplyDoubleArrayList dataPoints = simpleTimeSeries.getDataPoints();

    if (timestamps.size() == 0) {
      return StorableBuffer.EMPTY;
    } else if (timestamps.size() == 1) {
      return new StorableBuffer()
      {
        @Override
        public void store(ByteBuffer byteBuffer)
        {
          byteBuffer.putLong(timestamps.getLong(0));
          byteBuffer.putDouble(dataPoints.getDouble(0));
        }

        @Override
        public int getSerializedSize()
        {
          return SINGLE_ITEM_LIST_SIZE_BYTES;
        }
      };
    } else {
      // list size is >= 2, so encode/decode classes below here do not need to handle list size 0/1 in efficient
      // manner. It will never be called in practice.
      StorableList encodedTimestamps = timeStampsEncoderDecoder.encode(timestamps);
      StorableList encodedValues = DoubleValuesEncoderDecoder.encode(dataPoints);

      headerWriter.reset()
                  .setTimestampsRle(encodedTimestamps.isRle())
                  .setValuesRle(encodedValues.isRle());

      int sizeBytes = CellHeader.getSerializedSize()
                      + encodedTimestamps.getSerializedSize()
                      + encodedValues.getSerializedSize();

      return new StorableBuffer()
      {
        @Override
        public void store(ByteBuffer byteBuffer)
        {
          headerWriter.store(byteBuffer);
          encodedTimestamps.store(byteBuffer);
          encodedValues.store(byteBuffer);

        }

        @Override
        public int getSerializedSize()
        {
          return sizeBytes;
        }
      };
    }
  }


  /**
   * @param byteBuffer - must be in order(ByteOrder.nativeOrder()). position() is restored after reading
   * @return deserialized {@link SimpleTimeSeries}
   */
  @Override
  public SimpleTimeSeries deserialize(ByteBuffer byteBuffer)
  {
    if (byteBuffer.remaining() == 0) {
      return null;
    } else if (byteBuffer.remaining() == SINGLE_ITEM_LIST_SIZE_BYTES) {
      ImplyLongArrayList timestamps = new ImplyLongArrayList();

      timestamps.add(byteBuffer.getLong());

      ImplyDoubleArrayList values = new ImplyDoubleArrayList();

      values.add(byteBuffer.getDouble());

      SimpleTimeSeries simpleTimeSeries = new SimpleTimeSeries(
          timestamps,
          values,
          SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW,
          1
      );

      return simpleTimeSeries;
    } else {
      headerReader.readBytes(byteBuffer);

      Preconditions.checkState(
          headerReader.getVersion() == EXPECTED_ROW_HEADER_VERSION,
          "row header version mismatch. expected %s, got %s",
          EXPECTED_ROW_HEADER_VERSION,
          headerReader.getVersion()
      );

      ImplyLongArrayList timestamps = timeStampsEncoderDecoder.decode(byteBuffer, headerReader.isTimestampsRle());
      ImplyDoubleArrayList datapoints = DoubleValuesEncoderDecoder.decode(byteBuffer, headerReader.isValuesRle());

      return new SimpleTimeSeries(
          timestamps,
          datapoints,
          SimpleTimeSeriesComplexMetricSerde.ALL_TIME_WINDOW,
          timestamps.size()
      );
    }
  }
}
