/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import java.nio.ByteBuffer;

public class TimestampDecoderIterator implements LongIterator
{
  private final boolean isRle;
  private TimestampRawDecoderIterator rawDecoderIterator;
  private TimestampsRleDecoderIterator rleDecoderIterator;

  private TimestampDecoderIterator(boolean isRle, boolean useInteger, long minTimestamp, ByteBuffer timestampsBuffer)
  {
    this.isRle = isRle;

    if (isRle) {
      rleDecoderIterator = new TimestampsRleDecoderIterator(useInteger, minTimestamp, timestampsBuffer);
    } else {
      rawDecoderIterator = new TimestampRawDecoderIterator(useInteger, minTimestamp, timestampsBuffer);
    }
  }

  public static TimestampDecoderIterator createRleIntegerDecoderIterator(
      long minTimestamp,
      ByteBuffer timestampsBuffer
  )
  {
    return new TimestampDecoderIterator(true, true, minTimestamp, timestampsBuffer);
  }

  public static TimestampDecoderIterator createRawListIntegerDecoderIterator(
      long minTimestamp,
      ByteBuffer timestampsBuffer
  )
  {
    return new TimestampDecoderIterator(false, true, minTimestamp, timestampsBuffer);
  }

  public static TimestampDecoderIterator createRleLongDecoderIterator(
      long minTimestamp,
      ByteBuffer timestampsBuffer
  )
  {
    return new TimestampDecoderIterator(true, false, minTimestamp, timestampsBuffer);
  }

  public static TimestampDecoderIterator createRawListLongDecoderIterator(
      long minTimestamp,
      ByteBuffer timestampsBuffer
  )
  {
    return new TimestampDecoderIterator(false, false, minTimestamp, timestampsBuffer);
  }

  @Override
  public boolean hasNext()
  {
    return isRle ? rleDecoderIterator.hasNext() : rawDecoderIterator.hasNext();
  }

  @Override
  public long next()
  {
    return isRle ? rleDecoderIterator.next() : rawDecoderIterator.next();
  }
}
