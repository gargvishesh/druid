/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.serde.Serializer;
import org.apache.druid.segment.writeout.SegmentWriteOutMedium;
import org.apache.druid.segment.writeout.WriteOutBytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Set;

/**
 * Binary serialization for nested field type info, translated into this compact format for storage in segments.
 * The index of the type info here is the same as the field index in {@link NestedDataComplexColumnV1#fields}
 */
public class NestedLiteralTypeInfo
{
  private static final byte STRING_MASK = 1;
  private static final byte LONG_MASK = 1 << 2;
  private static final byte DOUBLE_MASK = 1 << 3;

  public static NestedLiteralTypeInfo read(ByteBuffer buffer, int length)
  {
    NestedLiteralTypeInfo typeInfo = new NestedLiteralTypeInfo(buffer);
    buffer.position(buffer.position() + length);
    return typeInfo;
  }

  private final ByteBuffer buffer;
  private final int startOffset;

  public NestedLiteralTypeInfo(ByteBuffer buffer)
  {
    this.buffer = buffer;
    this.startOffset = buffer.position();
  }

  public TypeSet getTypes(int fieldIndex)
  {
    return new TypeSet(buffer.get(startOffset + fieldIndex));
  }

  public static class TypeSet
  {
    private final byte types;

    public TypeSet(byte types)
    {
      this.types = types;
    }

    /**
     * If the set contains only a single {@link ColumnType}, return it, else null
     */
    @Nullable
    public ColumnType getSingleType()
    {
      return NestedLiteralTypeInfo.getSingleType(types);
    }

    public byte getValue()
    {
      return types;
    }

    @Override
    public String toString()
    {
      return convertToActualSet(types).toString();
    }
  }

  public static class MutableTypeSet
  {
    private byte types;

    public MutableTypeSet()
    {
      this((byte) 0x00);
    }

    public MutableTypeSet(byte types)
    {
      this.types = types;
    }

    public MutableTypeSet add(ColumnType type)
    {
      switch (type.getType()) {
        case STRING:
          types |= STRING_MASK;
          break;
        case LONG:
          types |= LONG_MASK;
          break;
        case DOUBLE:
          types |= DOUBLE_MASK;
          break;
        default:
          throw new ISE("Unsupported nested type: [%s]", type.asTypeString());
      }
      return this;
    }

    public MutableTypeSet merge(byte other)
    {
      types |= other;
      return this;
    }

    @Nullable
    public ColumnType getSingleType()
    {
      return NestedLiteralTypeInfo.getSingleType(types);
    }

    public byte getValue()
    {
      return types;
    }

    @Override
    public String toString()
    {
      return convertToActualSet(types).toString();
    }
  }

  public static class Writer implements Serializer
  {
    private final SegmentWriteOutMedium segmentWriteOutMedium;
    @Nullable
    private WriteOutBytes valuesOut = null;
    private int numWritten = 0;

    public Writer(SegmentWriteOutMedium segmentWriteOutMedium)
    {
      this.segmentWriteOutMedium = segmentWriteOutMedium;
    }

    public void open() throws IOException
    {
      this.valuesOut = segmentWriteOutMedium.makeWriteOutBytes();
    }

    public void write(NestedLiteralTypeInfo.MutableTypeSet types) throws IOException
    {
      valuesOut.write(types.getValue());
      numWritten++;
    }

    @Override
    public long getSerializedSize()
    {
      return numWritten;
    }

    @Override
    public void writeTo(WritableByteChannel channel, FileSmoosher smoosher) throws IOException
    {
      valuesOut.writeTo(channel);
    }
  }

  @Nullable
  private static ColumnType getSingleType(byte types)
  {
    int count = 0;
    ColumnType singleType = null;
    if ((types & STRING_MASK) > 0) {
      singleType = ColumnType.STRING;
      count++;
    }
    if ((types & LONG_MASK) > 0) {
      singleType = ColumnType.LONG;
      count++;
    }
    if ((types & DOUBLE_MASK) > 0) {
      singleType = ColumnType.DOUBLE;
      count++;
    }
    return count == 1 ? singleType : null;
  }

  private static Set<ColumnType> convertToActualSet(byte types)
  {
    final Set<ColumnType> theTypes = Sets.newHashSetWithExpectedSize(4);
    if ((types & STRING_MASK) > 0) {
      theTypes.add(ColumnType.STRING);
    }
    if ((types & LONG_MASK) > 0) {
      theTypes.add(ColumnType.LONG);
    }
    if ((types & DOUBLE_MASK) > 0) {
      theTypes.add(ColumnType.DOUBLE);
    }
    return theTypes;
  }
}
