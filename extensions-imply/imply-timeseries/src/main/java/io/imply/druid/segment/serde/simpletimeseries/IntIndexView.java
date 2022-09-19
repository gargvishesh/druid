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

import java.nio.ByteBuffer;

public class IntIndexView
{
  private final ByteBuffer byteBuffer;
  private final int numberOfEntries;

  public IntIndexView(ByteBuffer byteBuffer)
  {
    this.byteBuffer = byteBuffer;
    numberOfEntries = byteBuffer.remaining() / Integer.BYTES;
  }

  public int getStart(int entryNumber)
  {
    Preconditions.checkArgument(
        entryNumber < numberOfEntries, "invalid entry number %s [%s]", entryNumber, numberOfEntries
    );
    int start = byteBuffer.getInt(byteBuffer.position() + entryNumber * Integer.BYTES);

    return start;
  }

  public EntrySpan getEntrySpan(int entryNumber)
  {
    Preconditions.checkArgument(
        entryNumber < numberOfEntries, "invalid entry number %s [%s]", entryNumber, numberOfEntries
    );
    int start = byteBuffer.getInt(byteBuffer.position() + entryNumber * Integer.BYTES);
    int nextStart = byteBuffer.getInt(byteBuffer.position() + ((entryNumber + 1) * Integer.BYTES));

    return new EntrySpan(start, nextStart - start);
  }

  public static class EntrySpan
  {
    private final int start;
    private final int size;

    public EntrySpan(int start, int size)
    {
      this.start = start;
      this.size = size;
    }

    public int getStart()
    {
      return start;
    }

    public int getSize()
    {
      return size;
    }
  }
}
