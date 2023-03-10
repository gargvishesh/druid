/*
 *
 *  * Copyright (c) Imply Data, Inc. All rights reserved.
 *  *
 *  * This software is the confidential and proprietary information
 *  * of Imply Data, Inc. You shall not disclose such Confidential
 *  * Information and shall use it only in accordance with the terms
 *  * of the license agreement you entered into with Imply.
 *
 *
 */

package io.imply.druid.inet.column;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.DictionaryEncodedValueIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.ColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSerializer;
import org.apache.druid.segment.data.CompressionFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.data.RoaringBitmapSerdeFactory;
import org.apache.druid.segment.data.SingleValueColumnarIntsSerializer;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class IpComplexTypeSerdeTest
{
  BitmapSerdeFactory roaringFactory = RoaringBitmapSerdeFactory.getInstance();

  static {
    NullHandling.initializeForTests();
  }

  @Test
  public void testDeserializeColumnIpAddressWithVersion0AndGoodBitmap() throws Exception
  {
    IpAddressComplexTypeSerde serde = new IpAddressComplexTypeSerde();
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(ValueType.COMPLEX)
        .setHasMultipleValues(false);

    ByteBuffer buffer = ByteBuffer.allocate(1 << 12);
    buffer.put(ByteBuffer.wrap(new byte[]{0x00}));
    writeMetadataToBuffer(buffer);
    IpAddressTestUtils.writeIpAddressDictionary(buffer);
    writeEncodedValueToBuffer(buffer);
    writeGoodBitmapToBuffer(buffer);
    buffer.position(0);
    serde.deserializeColumn(buffer, builder, null);
    ColumnHolder columnHolder = builder.build();
    ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
    Assert.assertNotNull(indexSupplier);
    Assert.assertNotNull(indexSupplier.as(DictionaryEncodedValueIndex.class));
  }

  @Test
  public void testDeserializeColumnIpPrefixWithVersion0AndGoodBitmap() throws Exception
  {
    IpPrefixComplexTypeSerde serde = new IpPrefixComplexTypeSerde();
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(ValueType.COMPLEX)
        .setHasMultipleValues(false);

    ByteBuffer buffer = ByteBuffer.allocate(1 << 12);
    buffer.put(ByteBuffer.wrap(new byte[]{0x00}));
    writeMetadataToBuffer(buffer);
    IpAddressTestUtils.writeIpPrefixDictionary(buffer);
    writeEncodedValueToBuffer(buffer);
    writeGoodBitmapToBuffer(buffer);
    buffer.position(0);
    serde.deserializeColumn(buffer, builder, null);
    ColumnHolder columnHolder = builder.build();
    ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
    Assert.assertNotNull(indexSupplier);
    Assert.assertNotNull(indexSupplier.as(DictionaryEncodedValueIndex.class));
  }

  @Test
  public void testDeserializeColumnIpAddressWithVersion0AndBadBitmap() throws Exception
  {
    IpAddressComplexTypeSerde serde = new IpAddressComplexTypeSerde();
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(ValueType.COMPLEX)
        .setHasMultipleValues(false);

    ByteBuffer buffer = ByteBuffer.allocate(1 << 12);
    buffer.put(ByteBuffer.wrap(new byte[]{0x00}));
    writeMetadataToBuffer(buffer);
    IpAddressTestUtils.writeIpAddressDictionary(buffer);
    writeEncodedValueToBuffer(buffer);
    writeBadBitmapToBuffer(buffer);
    buffer.position(0);
    serde.deserializeColumn(buffer, builder, null);
    ColumnHolder columnHolder = builder.build();
    ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
    Assert.assertNotNull(indexSupplier);
    Assert.assertNull(indexSupplier.as(DictionaryEncodedValueIndex.class));
  }

  @Test
  public void testDeserializeColumnIpPrefixWithVersion0AndBadBitmap() throws Exception
  {
    IpPrefixComplexTypeSerde serde = new IpPrefixComplexTypeSerde();
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(ValueType.COMPLEX)
        .setHasMultipleValues(false);

    ByteBuffer buffer = ByteBuffer.allocate(1 << 12);
    buffer.put(ByteBuffer.wrap(new byte[]{0x00}));
    writeMetadataToBuffer(buffer);
    IpAddressTestUtils.writeIpPrefixDictionary(buffer);
    writeEncodedValueToBuffer(buffer);
    writeBadBitmapToBuffer(buffer);
    buffer.position(0);
    serde.deserializeColumn(buffer, builder, null);
    ColumnHolder columnHolder = builder.build();
    ColumnIndexSupplier indexSupplier = columnHolder.getIndexSupplier();
    Assert.assertNotNull(indexSupplier);
    Assert.assertNull(indexSupplier.as(DictionaryEncodedValueIndex.class));
  }

  private void writeMetadataToBuffer(ByteBuffer buffer) throws Exception
  {
    IndexSpec indexSpec = new IndexSpec(
        new BitmapSerde.DefaultBitmapSerdeFactory(),
        CompressionStrategy.LZ4,
        CompressionStrategy.LZF,
        CompressionFactory.LongEncodingStrategy.LONGS
    );
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    new SerializerUtils().writeString(
        baos,
        IpAddressComplexTypeSerde.JSON_MAPPER.writeValueAsString(
            new IpAddressBlobColumnMetadata(indexSpec.getBitmapSerdeFactory())
        )
    );
    buffer.put(ByteBuffer.wrap(baos.toByteArray()));
  }

  private void writeEncodedValueToBuffer(ByteBuffer buffer) throws Exception
  {
    ColumnarIntsSerializer encodedValueSerializer = CompressedVSizeColumnarIntsSerializer.create(
        "ip",
        new OnHeapMemorySegmentWriteOutMedium(),
        "ip",
        4,
        CompressionStrategy.LZ4
    );
    encodedValueSerializer.open();
    ((SingleValueColumnarIntsSerializer) encodedValueSerializer).addValue(0);
    IpAddressTestUtils.writeToBuffer(buffer, encodedValueSerializer);
  }

  private void writeGoodBitmapToBuffer(ByteBuffer buffer) throws Exception
  {
    IpAddressTestUtils.writeBitmap(buffer);
  }

  private void writeBadBitmapToBuffer(ByteBuffer buffer) throws Exception
  {
    GenericIndexedWriter<ImmutableBitmap> bitmapWriter = new GenericIndexedWriter<>(
        new OnHeapMemorySegmentWriteOutMedium(),
        "bitmaps",
        roaringFactory.getObjectStrategy()
    );
    bitmapWriter.setObjectsNotSorted();
    bitmapWriter.open();
    bitmapWriter.write(null);
    IpAddressTestUtils.writeToBuffer(buffer, bitmapWriter);
  }
}
