/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.api.client.util.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.WritableSupplier;
import org.apache.druid.segment.serde.ColumnPartSerde;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

public class NestedDataComplexColumn implements ComplexColumn
{
  @Nullable
  public static NestedDataComplexColumn fromColumnSelector(
      ColumnSelector columnSelector,
      String columnName
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(columnName);
    if (holder == null) {
      return null;
    }
    BaseColumn theColumn = holder.getColumn();
    if (!(theColumn instanceof NestedDataComplexColumn)) {
      throw new IAE(
          "Column [%s] is invalid type, found [%s] instead of [%s]",
          columnName,
          theColumn.getClass(),
          NestedDataComplexColumn.class.getSimpleName()
      );
    }
    return (NestedDataComplexColumn) theColumn;
  }

  private final NestedDataColumnMetadata metadata;
  private final ColumnConfig columnConfig;
  final GenericIndexed<StructuredData> rawColumn;
  final ImmutableBitmap nullValues;
  final GenericIndexed<String> fields;
  final GenericIndexed<String> dictionary;
  final SmooshedFileMapper fileMapper;

  private final ConcurrentHashMap<String, ColumnHolder> columns = new ConcurrentHashMap<>();

  public NestedDataComplexColumn(
      NestedDataColumnMetadata metadata,
      GenericIndexed<StructuredData> raw,
      ImmutableBitmap nullValues,
      GenericIndexed<String> fields,
      GenericIndexed<String> dictionary,
      ColumnConfig columnConfig,
      SmooshedFileMapper fileMapper
  )
  {
    this.metadata = metadata;
    this.rawColumn = raw;
    this.nullValues = nullValues;
    this.fields = fields;
    this.dictionary = dictionary;
    this.columnConfig = columnConfig;
    this.fileMapper = fileMapper;
  }

  @Override
  public Class<?> getClazz()
  {
    return StructuredData.class;
  }

  @Override
  public String getTypeName()
  {
    return NestedDataComplexTypeSerde.TYPE_NAME;
  }

  @SuppressWarnings("NullableProblems")
  @Nullable
  @Override
  public Object getRowValue(int rowNum)
  {
    if (nullValues.get(rowNum)) {
      return null;
    }
    return rawColumn.get(rowNum);
  }

  @Override
  public int getLength()
  {
    return 0;
  }

  @Override
  public void close()
  {

  }

  public DimensionSelector makeDimensionSelector(String field, ReadableOffset readableOffset, ExtractionFn fn)
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      NestedFieldStringDictionaryEncodedColumn col = (NestedFieldStringDictionaryEncodedColumn) getColumn(field).getColumn();
      return col.makeDimensionSelector(readableOffset, fn);
    } else {
      return DimensionSelector.constant(null);
    }
  }

  public ColumnValueSelector<?> makeColumnValueSelector(String field, ReadableOffset readableOffset)
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      NestedFieldStringDictionaryEncodedColumn col = (NestedFieldStringDictionaryEncodedColumn) getColumn(field).getColumn();
      return col.makeColumnValueSelector(readableOffset);
    } else {
      return NilColumnValueSelector.instance();
    }
  }

  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      String field,
      ReadableVectorOffset readableOffset
  )
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      NestedFieldStringDictionaryEncodedColumn col = (NestedFieldStringDictionaryEncodedColumn) getColumn(field).getColumn();
      return col.makeSingleValueDimensionVectorSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  public VectorObjectSelector makeVectorObjectSelector(String field, ReadableVectorOffset readableOffset)
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      NestedFieldStringDictionaryEncodedColumn col = (NestedFieldStringDictionaryEncodedColumn) getColumn(field).getColumn();
      return col.makeVectorObjectSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  public BitmapIndex makeBitmapIndex(String field)
  {
    return getColumn(field).getBitmapIndex();
  }

  private ColumnHolder getColumn(String field)
  {
    return columns.computeIfAbsent(field, this::readStringDictionaryEncodedColumn);
  }

  private ColumnHolder readStringDictionaryEncodedColumn(String field)
  {
    try {
      ByteBuffer dataBuffer = fileMapper.mapFile(
          NestedDataColumnSerializer.getFieldFileName(
              field,
              metadata.getFileNameBase()
          )
      );
      if (dataBuffer == null) {
        throw new ISE("Can't find field [%s] in [%s] file.", field, metadata.getFileNameBase());
      }

      ColumnBuilder columnBuilder = new ColumnBuilder().setFileMapper(fileMapper);
      // heh, maybe this should be its own class, or DictionaryEncodedColumnPartSerde could be cooler
      ColumnPartSerde.Deserializer deserializer = new ColumnPartSerde.Deserializer()
      {
        @Override
        public void read(ByteBuffer buffer, ColumnBuilder builder, ColumnConfig columnConfig)
        {
          DictionaryEncodedColumnPartSerde.VERSION version = DictionaryEncodedColumnPartSerde.VERSION.fromByte(
              buffer.get()
          );
          // we should check this someday soon, but for now just read it to push the buffer position ahead
          int flags = buffer.getInt();
          Preconditions.checkState(
              flags == DictionaryEncodedColumnPartSerde.NO_FLAGS,
              StringUtils.format(
                  "Unrecognized bits set in space reserved for future flags for field column [%s]",
                  field
              )
          );

          final GenericIndexed<Integer> localDictionary = GenericIndexed.read(
              buffer,
              NestedFieldStringDictionaryEncodedColumn.makeDictionaryStrategy(metadata.getByteOrder()),
              builder.getFileMapper()
          );
          final WritableSupplier<ColumnarInts> ints;
          if (version == DictionaryEncodedColumnPartSerde.VERSION.COMPRESSED) {
            ints = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(buffer, metadata.getByteOrder());
          } else {
            ints = VSizeColumnarInts.readFromByteBuffer(buffer);
          }
          builder.setType(ValueType.STRING);
          final String firstDictionaryEntry = dictionary.get(localDictionary.get(0));
          Supplier<DictionaryEncodedColumn<?>> columnSupplier = () ->
              new NestedFieldStringDictionaryEncodedColumn(ints.get(), dictionary, localDictionary);
          builder.setHasMultipleValues(false)
                 .setHasNulls(firstDictionaryEntry == null)
                 .setDictionaryEncodedColumnSupplier(columnSupplier);
          GenericIndexed<ImmutableBitmap> rBitmaps = GenericIndexed.read(
              buffer,
              metadata.getBitmapSerdeFactory().getObjectStrategy(),
              builder.getFileMapper()
          );
          builder.setBitmapIndex(
              new NestedFieldStringBitmapIndexSupplier(
                  metadata.getBitmapSerdeFactory().getBitmapFactory(),
                  rBitmaps,
                  localDictionary,
                  dictionary
              )
          );
        }
      };
      deserializer.read(dataBuffer, columnBuilder, columnConfig);
      return columnBuilder.build();
    }
    catch (IOException ex) {
      throw new RE(ex, "Failed to read data for [%s]", field);
    }
  }
}
