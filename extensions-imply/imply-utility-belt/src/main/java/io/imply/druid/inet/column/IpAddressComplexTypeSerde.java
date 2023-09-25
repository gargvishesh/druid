/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.imply.druid.inet.IpAddressModule;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.TypeStrategy;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.WritableSupplier;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static io.imply.druid.inet.column.IpAddressDictionaryEncodedColumnMerger.SERIALIZER_UTILS;

public class IpAddressComplexTypeSerde extends ComplexMetricSerde
{
  static ObjectStrategy<ByteBuffer> NULLABLE_BYTE_BUFFER_STRATEGY = new ObjectStrategy<ByteBuffer>()
  {
    @Override
    public Class<? extends ByteBuffer> getClazz()
    {
      return ByteBuffer.class;
    }

    @Nullable
    @Override
    public ByteBuffer fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      if (numBytes == 0) {
        return null;
      }
      final ByteBuffer dup = buffer.asReadOnlyBuffer();
      dup.limit(buffer.position() + numBytes);
      return dup;
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable ByteBuffer val)
    {
      if (val == null) {
        return null;
      }
      // copy defensively
      final ByteBuffer dup = val.duplicate();
      final byte[] bytes = new byte[dup.remaining()];
      dup.get(bytes);
      return bytes;
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
      // this is signed comparison and sad, but matches the way IpAddressBlob and IpPrefixBlob comparison is implemented
      return Comparators.<ByteBuffer>naturalNullsFirst().compare(o1, o2);
    }
  };

  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  public static final IpAddressComplexTypeSerde INSTANCE = new IpAddressComplexTypeSerde();

  @Override
  public String getTypeName()
  {
    return IpAddressModule.ADDRESS_TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    // getExtractor is only used for complex aggregators at ingest time
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public TypeStrategy<IpAddressBlob> getTypeStrategy()
  {
    return new TypeStrategy<IpAddressBlob>()
    {
      @Override
      public int estimateSizeBytes(IpAddressBlob value)
      {
        return IpAddressBlob.SIZE;
      }

      @Override
      public IpAddressBlob read(ByteBuffer buffer)
      {
        final IpAddressBlob blob = IpAddressBlob.ofByteBuffer(buffer);
        buffer.position(buffer.position() + IpAddressBlob.SIZE);
        return blob;
      }

      @Override
      public boolean readRetainsBufferReference()
      {
        return false;
      }

      @Override
      public int write(ByteBuffer buffer, IpAddressBlob value, int maxSizeBytes)
      {
        if (maxSizeBytes < IpAddressBlob.SIZE) {
          return maxSizeBytes - IpAddressBlob.SIZE;
        }
        buffer.put(value.getBytes());
        return IpAddressBlob.SIZE;
      }

      @Override
      public int compare(Object o1, Object o2)
      {
        return getObjectStrategy().compare(o1, o2);
      }
    };
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public void deserializeColumn(
      ByteBuffer buffer,
      ColumnBuilder builder,
      ColumnConfig columnConfig
  )
  {
    try {
      byte version = buffer.get();
      Preconditions.checkArgument(version == 0 || version == 1, StringUtils.format("Unknown version %s", version));
      IpAddressBlobColumnMetadata metadata = IpAddressComplexTypeSerde.JSON_MAPPER.readValue(
          SERIALIZER_UTILS.readString(buffer),
          IpAddressBlobColumnMetadata.class
      );

      final GenericIndexed<ByteBuffer> dictionaryBytes = GenericIndexed.read(
          buffer,
          NULLABLE_BYTE_BUFFER_STRATEGY,
          builder.getFileMapper()
      );

      // ip address will never be multi-valued, so its either compressed or not
      final WritableSupplier<ColumnarInts> column =
          buffer.get(buffer.position()) == VSizeColumnarInts.VERSION
          ? VSizeColumnarInts.readFromByteBuffer(buffer)
          : CompressedVSizeColumnarIntsSupplier.fromByteBuffer(buffer, ByteOrder.nativeOrder());

      GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(
          buffer,
          metadata.getBitmapSerdeFactory().getObjectStrategy(),
          builder.getFileMapper()
      );

      boolean hasValidBitmap = true;
      if (version == 0) {
        // Version 0 has a bug where the bitmap can be null after segments are merged during ingestion.
        // We should not use these bitmaps and fall back to using no indexes
        for (ImmutableBitmap bitmap : bitmaps) {
          if (bitmap == null) {
            hasValidBitmap = false;
          }
        }
      }
      if (hasValidBitmap) {
        builder.setIndexSupplier(
            new DictionaryEncodedIpAddressBlobColumnIndexSupplier(
                metadata.getBitmapSerdeFactory().getBitmapFactory(),
                bitmaps,
                dictionaryBytes
            ),
            true,
            false
        );
      }

      IpAddressDictionaryEncodedColumnSupplier supplier = new IpAddressDictionaryEncodedColumnSupplier(
          column,
          dictionaryBytes
      );

      builder.setDictionaryEncodedColumnSupplier(supplier);
      builder.setType(ValueType.COMPLEX);
      builder.setComplexTypeName(IpAddressModule.ADDRESS_TYPE_NAME);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return IpAddressBlob.STRATEGY;
  }
}
