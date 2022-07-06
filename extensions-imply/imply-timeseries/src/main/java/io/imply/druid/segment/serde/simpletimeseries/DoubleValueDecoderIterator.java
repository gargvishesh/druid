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

public class DoubleValueDecoderIterator implements DoubleIterator
{
  private final boolean isRle;
  private DoubleValueRawDecoderIterator rawDecoderIterator;
  private DoubleValueRleDecoderIterator rleDecoderIterator;

  private DoubleValueDecoderIterator(boolean isRle, ByteBuffer datapointsBuffer)
  {
    this.isRle = isRle;

    if (isRle) {
      rleDecoderIterator = new DoubleValueRleDecoderIterator(datapointsBuffer);
    } else {
      rawDecoderIterator = new DoubleValueRawDecoderIterator(datapointsBuffer);
    }
  }

  public static DoubleValueDecoderIterator createRleDecoderIterator(ByteBuffer timestampsBuffer)
  {
    return new DoubleValueDecoderIterator(true, timestampsBuffer);
  }

  public static DoubleValueDecoderIterator createRawListDecoderIterator(ByteBuffer timestampsBuffer)
  {
    return new DoubleValueDecoderIterator(false, timestampsBuffer);
  }

  @Override
  public boolean hasNext()
  {
    return isRle ? rleDecoderIterator.hasNext() : rawDecoderIterator.hasNext();
  }

  @Override
  public double next()
  {
    return isRle ? rleDecoderIterator.next() : rawDecoderIterator.next();
  }
}
