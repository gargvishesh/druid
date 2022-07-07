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
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ObjectStrategy;
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
  private static ObjectStrategy<ByteBuffer> NULLABLE_BYTE_BUFFER_STRATEGY = new ObjectStrategy<ByteBuffer>()
  {
    @Override
    public Class<? extends ByteBuffer> getClazz()
    {
      return GenericIndexed.BYTE_BUFFER_STRATEGY.getClazz();
    }

    @Nullable
    @Override
    public ByteBuffer fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      if (numBytes == 0) {
        return null;
      }
      return GenericIndexed.BYTE_BUFFER_STRATEGY.fromByteBuffer(buffer, numBytes);
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable ByteBuffer val)
    {
      return GenericIndexed.BYTE_BUFFER_STRATEGY.toBytes(val);
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
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
      Preconditions.checkArgument(version == 0, StringUtils.format("Unknown version %s", version));
      IpAddressBlobColumnMetadata metadata = IpAddressComplexTypeSerde.JSON_MAPPER.readValue(
          SERIALIZER_UTILS.readString(buffer),
          IpAddressBlobColumnMetadata.class
      );

      final GenericIndexed<ByteBuffer> dictionaryBytes = GenericIndexed.read(
          buffer,
          NULLABLE_BYTE_BUFFER_STRATEGY,
          builder.getFileMapper()
      );

      final WritableSupplier<ColumnarInts> column = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(
          buffer,
          ByteOrder.nativeOrder()
      );

      GenericIndexed<ImmutableBitmap> bitmaps = GenericIndexed.read(
          buffer,
          metadata.getBitmapSerdeFactory().getObjectStrategy(),
          builder.getFileMapper()
      );
      builder.setIndexSupplier(
          new DictionaryEncodedIpAddressBlobColumnIndexSupplier(
              metadata.getBitmapSerdeFactory().getBitmapFactory(),
              bitmaps,
              dictionaryBytes
          ),
          true,
          false
      );
      IpAddressDictionaryEncodedColumnSupplier supplier = new IpAddressDictionaryEncodedColumnSupplier(
          column,
          dictionaryBytes,
          bitmaps
      );

      builder.setDictionaryEncodedColumnSupplier(supplier);
      builder.setType(ValueType.COMPLEX);
      builder.setComplexTypeName(IpAddressModule.ADDRESS_TYPE_NAME);
      builder.setFilterable(true);
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
