/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import io.imply.druid.talaria.counters.ChannelCounters;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.IOException;

public class CountableInputSourceReader implements InputSourceReader
{
  private final InputSourceReader inputSourceReader;
  private final ChannelCounters channelCounters;

  public CountableInputSourceReader(
      final InputSourceReader inputSourceReader,
      final ChannelCounters channelCounters
  )
  {
    this.inputSourceReader = inputSourceReader;
    this.channelCounters = channelCounters;
  }

  @Override
  public CloseableIterator<InputRow> read() throws IOException
  {
    return inputSourceReader.read().map(inputRow -> {
      channelCounters.incrementRowCount();
      return inputRow;
    });
  }

  @Override
  public CloseableIterator<InputRowListPlusRawValues> sample() throws IOException
  {
    return inputSourceReader.sample();
  }
}
