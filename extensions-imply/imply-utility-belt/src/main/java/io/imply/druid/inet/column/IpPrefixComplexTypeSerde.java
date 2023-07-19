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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.imply.druid.inet.IpAddressModule;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IpPrefixComplexTypeSerde extends ComplexMetricSerde
{
  public static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  public static final IpPrefixComplexTypeSerde INSTANCE = new IpPrefixComplexTypeSerde();

  @Override
  public String getTypeName()
  {
    return IpAddressModule.PREFIX_TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    // getExtractor is only used for complex aggregators at ingest time
    throw new UnsupportedOperationException("Not Supported");
  }

  @Override
  public TypeStrategy<IpPrefixBlob> getTypeStrategy()
  {
    return new TypeStrategy<IpPrefixBlob>()
    {
      @Override
      public int estimateSizeBytes(IpPrefixBlob value)
      {
        return IpPrefixBlob.SIZE;
      }

      @Override
      public IpPrefixBlob read(ByteBuffer buffer)
      {
        final IpPrefixBlob blob = IpPrefixBlob.ofByteBuffer(buffer);
        buffer.position(buffer.position() + IpPrefixBlob.SIZE);
        return blob;
      }

      @Override
      public boolean readRetainsBufferReference()
      {
        return false;
      }

      @Override
      public int write(ByteBuffer buffer, IpPrefixBlob value, int maxSizeBytes)
      {
        if (maxSizeBytes < IpPrefixBlob.SIZE) {
          return maxSizeBytes - IpPrefixBlob.SIZE;
        }
        buffer.put(value.getBytes());
        return IpPrefixBlob.SIZE;
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
      IpAddressBlobColumnMetadata metadata = IpPrefixComplexTypeSerde.JSON_MAPPER.readValue(
          IpPrefixDictionaryEncodedColumnMerger.SERIALIZER_UTILS.readString(buffer),
          IpAddressBlobColumnMetadata.class
      );

      final GenericIndexed<ByteBuffer> dictionaryBytes = GenericIndexed.read(
          buffer,
          IpAddressComplexTypeSerde.NULLABLE_BYTE_BUFFER_STRATEGY,
          builder.getFileMapper()
      );

      // ip prefix will never be multi-valued, so its either compressed or not
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

      IpPrefixDictionaryEncodedColumnSupplier supplier = new IpPrefixDictionaryEncodedColumnSupplier(
          column,
          dictionaryBytes
      );

      builder.setDictionaryEncodedColumnSupplier(supplier);
      builder.setType(ValueType.COMPLEX);
      builder.setComplexTypeName(IpAddressModule.PREFIX_TYPE_NAME);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return IpPrefixBlob.STRATEGY;
  }
}
