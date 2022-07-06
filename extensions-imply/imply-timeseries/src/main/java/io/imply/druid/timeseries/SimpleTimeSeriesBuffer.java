/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.imply.druid.segment.serde.simpletimeseries.SimpleTimeSeriesSerde;

import java.nio.ByteBuffer;

public class SimpleTimeSeriesBuffer
{
  private final Supplier<ByteBuffer> byteBufferSupplier;
  private final SimpleTimeSeriesSerde timeSeriesSerde;

  private boolean isRead = false;
  private SimpleTimeSeries simpleTimeSeries = null;

  public SimpleTimeSeriesBuffer(SimpleTimeSeriesSerde timeSeriesSerde, Supplier<ByteBuffer> byteBufferSupplier)
  {
    this.timeSeriesSerde = timeSeriesSerde;
    this.byteBufferSupplier = Suppliers.memoize(byteBufferSupplier);
  }

  public SimpleTimeSeries getSimpleTimeSeries()
  {
    deserializeTimeSeriesIfNeeded();
    return simpleTimeSeries;
  }

  public boolean isNull()
  {
    return (isRead && timeSeriesSerde == null) || (byteBufferSupplier.get().remaining() == 0);
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
    SimpleTimeSeriesBuffer that = (SimpleTimeSeriesBuffer) o;
    return Objects.equal(getSimpleTimeSeries(), that.getSimpleTimeSeries());
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(getSimpleTimeSeries());
  }

  private void deserializeTimeSeriesIfNeeded()
  {
    if (!isRead) {
      Preconditions.checkNotNull(timeSeriesSerde, "invalid state: trying to deserialize without serde");
      simpleTimeSeries = timeSeriesSerde.deserialize(byteBufferSupplier.get());
      isRead = true;
    }
  }
}
