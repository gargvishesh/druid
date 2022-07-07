/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import io.imply.druid.nested.NestedDataModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ObjectStrategy;
import org.apache.druid.segment.serde.ComplexMetricExtractor;
import org.apache.druid.segment.serde.ComplexMetricSerde;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class NestedDataComplexTypeSerde extends ComplexMetricSerde
{
  public static final String TYPE_NAME = "json";
  public static final String GENERIC_TYPE_NAME = "nested";
  public static final ColumnType TYPE = ColumnType.ofComplex(TYPE_NAME);
  public static final ColumnType GENERIC_TYPE = ColumnType.ofComplex(GENERIC_TYPE_NAME);

  public static final ObjectMapper OBJECT_MAPPER;

  public static final NestedDataComplexTypeSerde INSTANCE = new NestedDataComplexTypeSerde();

  static {
    final SmileFactory smileFactory = new SmileFactory();
    smileFactory.configure(SmileGenerator.Feature.ENCODE_BINARY_AS_7BIT, false);
    smileFactory.delegateToTextual(true);
    final ObjectMapper mapper = new DefaultObjectMapper(smileFactory, null);
    mapper.getFactory().setCodec(mapper);
    mapper.registerModules(NestedDataModule.getJacksonModulesList());
    OBJECT_MAPPER = mapper;
  }

  @Override
  public String getTypeName()
  {
    return TYPE_NAME;
  }

  @Override
  public ComplexMetricExtractor getExtractor()
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder)
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public void deserializeColumn(
      ByteBuffer buffer,
      ColumnBuilder builder,
      ColumnConfig columnConfig
  )
  {
    NestedDataColumnSupplier supplier = new NestedDataColumnSupplier(buffer, builder, columnConfig, OBJECT_MAPPER);
    // todo (clint): upstream
    ColumnCapabilitiesImpl capabilitiesBuilder = builder.getCapabilitiesBuilder();
    capabilitiesBuilder.setDictionaryEncoded(true);
    capabilitiesBuilder.setDictionaryValuesSorted(true);
    capabilitiesBuilder.setDictionaryValuesUnique(true);
    builder.setComplexTypeName(TYPE_NAME);
    builder.setComplexColumnSupplier(supplier);
  }

  @Override
  public ObjectStrategy getObjectStrategy()
  {
    return new ObjectStrategy<Object>()
    {

      @Override
      public int compare(Object o1, Object o2)
      {
        return Comparators.<StructuredData>naturalNullsFirst().compare(StructuredData.possiblyWrap(o1), StructuredData.possiblyWrap(o2));
      }

      @Override
      public Class<? extends Object> getClazz()
      {
        return StructuredData.class;
      }

      @Nullable
      @Override
      public Object fromByteBuffer(ByteBuffer buffer, int numBytes)
      {
        final byte[] bytes = new byte[numBytes];
        buffer.get(bytes, 0, numBytes);
        try {
          return OBJECT_MAPPER.readValue(bytes, StructuredData.class);
        }
        catch (IOException e) {
          throw new ISE(e, "Unable to deserialize value");
        }
      }

      @Nullable
      @Override
      public byte[] toBytes(@Nullable Object val)
      {
        if (val == null) {
          return new byte[0];
        }
        try {
          return OBJECT_MAPPER.writeValueAsBytes(val);
        }
        catch (JsonProcessingException e) {
          throw new ISE(e, "Unable to serialize value [%s]", val);
        }
      }
    };
  }
}
