/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import com.google.common.primitives.Ints;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

public class RowIndexReader
{
  private final BlockCompressedPayloadReader payloadReader;

  public RowIndexReader(BlockCompressedPayloadReader payloadReader)
  {
    this.payloadReader = payloadReader;
  }

  public long getStart(int entryNumber)
  {
    int position = entryNumber * Long.BYTES;
    ByteBuffer payload = payloadReader.read(position, Long.BYTES);

    return payload.getLong();
  }

  @Nonnull
  public PayloadEntrySpan getEntrySpan(int entryNumber)
  {
    int position = entryNumber * Long.BYTES;
    ByteBuffer payload = payloadReader.read(position, 2 * Long.BYTES);
    long payloadValue = payload.getLong();
    long nextPayloadValue = payload.getLong();

    return new PayloadEntrySpan(payloadValue, Ints.checkedCast(nextPayloadValue - payloadValue));
  }
}
