/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.talaria.counters.CounterTracker;
import org.apache.druid.java.util.common.ISE;

import java.util.Map;
import java.util.function.Consumer;

/**
 * Reader that handles multiple types of slices.
 */
public class MapInputSliceReader implements InputSliceReader
{
  private final Map<Class<? extends InputSlice>, InputSliceReader> readerMap;

  @Inject
  public MapInputSliceReader(final Map<Class<? extends InputSlice>, InputSliceReader> readerMap)
  {
    this.readerMap = ImmutableMap.copyOf(readerMap);
  }

  @Override
  public int numReadableInputs(InputSlice slice)
  {
    return getReader(slice.getClass()).numReadableInputs(slice);
  }

  @Override
  public ReadableInputs attach(
      int inputNumber,
      InputSlice slice,
      CounterTracker counters,
      Consumer<Throwable> warningPublisher
  )
  {
    return getReader(slice.getClass()).attach(inputNumber, slice, counters, warningPublisher);
  }

  private InputSliceReader getReader(final Class<? extends InputSlice> clazz)
  {
    final InputSliceReader reader = readerMap.get(clazz);

    if (reader == null) {
      throw new ISE("Cannot handle inputSpec of class [%s]", clazz.getName());
    }

    return reader;
  }
}
