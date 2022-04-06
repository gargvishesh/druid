/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.GenericIndexed;

import java.io.IOException;
import java.nio.ByteBuffer;

public class NestedDataColumnSupplier implements Supplier<ComplexColumn>
{
  private final NestedDataColumnMetadata metadata;

  private final GenericIndexed<StructuredData> raw;
  private final ImmutableBitmap nullValues;
  private final GenericIndexed<String> fields;
  private final NestedLiteralTypeInfo fieldInfo;
  private final GenericIndexed<String> dictionary;
  private final FixedIndexed<Long> longDictionary;
  private final FixedIndexed<Double> doubleDictionary;

  private final ColumnConfig columnConfig;
  private final SmooshedFileMapper fileMapper;


  public NestedDataColumnSupplier(
      ByteBuffer bb,
      ColumnBuilder columnBuilder,
      ColumnConfig columnConfig,
      ObjectMapper jsonMapper
  )
  {
    byte version = bb.get();
    if (version == 0) {

      try {
        metadata = jsonMapper.readValue(
            IndexMerger.SERIALIZER_UTILS.readString(bb),
            NestedDataColumnMetadata.class
        );
        fields = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY);
        dictionary = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY);
        raw = GenericIndexed.read(bb, NestedDataComplexTypeSerde.INSTANCE.getObjectStrategy());
        if (metadata.hasNulls()) {
          nullValues = metadata.getBitmapSerdeFactory().getObjectStrategy().fromByteBufferWithSize(bb);
        } else {
          nullValues = metadata.getBitmapSerdeFactory().getBitmapFactory().makeEmptyImmutableBitmap();
        }
        longDictionary = null;
        doubleDictionary = null;
        fieldInfo = null;
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V0 column.");
      }
    } else if (version == 0x01) {
      try {
        metadata = jsonMapper.readValue(
            IndexMerger.SERIALIZER_UTILS.readString(bb),
            NestedDataColumnMetadata.class
        );
        fields = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY);
        fieldInfo = NestedLiteralTypeInfo.read(bb, fields.size());
        dictionary = GenericIndexed.read(bb, GenericIndexed.STRING_STRATEGY);
        longDictionary = FixedIndexed.read(bb, ColumnType.LONG.getStrategy(), metadata.getByteOrder(), Long.BYTES);
        doubleDictionary = FixedIndexed.read(bb, ColumnType.DOUBLE.getStrategy(), metadata.getByteOrder(), Double.BYTES);
        raw = GenericIndexed.read(bb, NestedDataComplexTypeSerde.INSTANCE.getObjectStrategy());
        if (metadata.hasNulls()) {
          nullValues = metadata.getBitmapSerdeFactory().getObjectStrategy().fromByteBufferWithSize(bb);
        } else {
          nullValues = metadata.getBitmapSerdeFactory().getBitmapFactory().makeEmptyImmutableBitmap();
        }
      }
      catch (IOException ex) {
        throw new RE(ex, "Failed to deserialize V0 column.");
      }
    } else {
      throw new RE("Unknown version" + version);
    }

    fileMapper = Preconditions.checkNotNull(columnBuilder.getFileMapper(), "Null fileMapper");

    this.columnConfig = columnConfig;
  }

  @Override
  public ComplexColumn get()
  {
    if (longDictionary != null) {
      return new NestedDataComplexColumnV1(
          metadata,
          columnConfig,
          raw,
          nullValues,
          fields,
          fieldInfo,
          dictionary,
          longDictionary,
          doubleDictionary,
          fileMapper
      );
    }
    return new NestedDataComplexColumnV0(
        metadata,
        raw,
        nullValues,
        fields,
        dictionary,
        columnConfig,
        fileMapper
    );
  }
}
