/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.collect.ImmutableList;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.Iterator;


@RunWith(Parameterized.class)
public class FixedIndexedTest extends InitializedNullHandlingTest
{
  private static final Long[] LONGS = new Long[64];

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{ByteOrder.LITTLE_ENDIAN}, new Object[]{ByteOrder.BIG_ENDIAN});
  }

  @BeforeClass
  public static void setup()
  {
    for (int i = 0; i < LONGS.length; i++) {
      LONGS[i] = i * 10L;
    }
  }

  private final ByteOrder order;

  public FixedIndexedTest(ByteOrder byteOrder)
  {
    this.order = byteOrder;
  }

  @Test
  public void testGet() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, false);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES);
    Assert.assertEquals(64, fixedIndexed.size());
    for (int i = 0; i < LONGS.length; i++) {
      Assert.assertEquals(LONGS[i], fixedIndexed.get(i));
      Assert.assertEquals(i, fixedIndexed.indexOf(LONGS[i]));
    }
  }

  @Test
  public void testIterator() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, false);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES);
    Iterator<Long> iterator = fixedIndexed.iterator();
    int i = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(LONGS[i++], iterator.next());
    }
  }

  @Test
  public void testGetWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, true);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES);
    Assert.assertEquals(65, fixedIndexed.size());
    Assert.assertNull(fixedIndexed.get(0));
    for (int i = 0; i < LONGS.length; i++) {
      Assert.assertEquals(LONGS[i], fixedIndexed.get(i + 1));
      Assert.assertEquals(i + 1, fixedIndexed.indexOf(LONGS[i]));
    }
  }

  @Test
  public void testIteratorWithNull() throws IOException
  {
    ByteBuffer buffer = ByteBuffer.allocate(1 << 14);
    fillBuffer(buffer, order, true);
    FixedIndexed<Long> fixedIndexed = FixedIndexed.read(buffer, ColumnType.LONG.getStrategy(), order, Long.BYTES);
    Iterator<Long> iterator = fixedIndexed.iterator();
    Assert.assertNull(iterator.next());
    int i = 0;
    while (iterator.hasNext()) {
      Assert.assertEquals(LONGS[i++], iterator.next());
    }
  }

  private static void fillBuffer(ByteBuffer buffer, ByteOrder order, boolean withNull) throws IOException
  {
    buffer.position(0);
    FixedIndexedWriter<Long> writer = new FixedIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        ColumnType.LONG.getStrategy(),
        order,
        Long.BYTES,
        true
    );
    writer.open();
    if (withNull) {
      writer.write(null);
    }
    for (Long aLong : LONGS) {
      writer.write(aLong);
    }
    WritableByteChannel channel = new WritableByteChannel()
    {
      @Override
      public int write(ByteBuffer src)
      {
        int size = src.remaining();
        buffer.put(src);
        return size;
      }

      @Override
      public boolean isOpen()
      {
        return true;
      }

      @Override
      public void close()
      {
      }
    };
    long size = writer.getSerializedSize();
    buffer.position(0);
    writer.writeTo(channel, null);
    Assert.assertEquals(size, buffer.position());
    buffer.position(0);
  }
}
