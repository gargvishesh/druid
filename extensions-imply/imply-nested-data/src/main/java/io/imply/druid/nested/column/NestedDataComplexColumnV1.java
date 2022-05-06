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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnBuilder;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.DictionaryEncodedColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ColumnarDoubles;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.ColumnarLongs;
import org.apache.druid.segment.data.CompressedColumnarDoublesSuppliers;
import org.apache.druid.segment.data.CompressedColumnarLongsSupplier;
import org.apache.druid.segment.data.CompressedVSizeColumnarIntsSupplier;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.data.VSizeColumnarInts;
import org.apache.druid.segment.data.WritableSupplier;
import org.apache.druid.segment.serde.DictionaryEncodedColumnPartSerde;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;

public class NestedDataComplexColumnV1 extends NestedDataComplexColumn
{
  private final NestedDataColumnMetadata metadata;
  private final ColumnConfig columnConfig;
  private final Closer closer;
  final GenericIndexed<StructuredData> rawColumn;
  final ImmutableBitmap nullValues;
  final GenericIndexed<String> fields;
  final NestedLiteralTypeInfo fieldInfo;
  final GenericIndexed<String> stringDictionary;
  final FixedIndexed<Long> longDictionary;
  final FixedIndexed<Double> doubleDictionary;
  final SmooshedFileMapper fileMapper;


  public NestedDataComplexColumnV1(
      NestedDataColumnMetadata metadata,
      ColumnConfig columnConfig,
      GenericIndexed<StructuredData> rawColumn,
      ImmutableBitmap nullValues,
      GenericIndexed<String> fields,
      NestedLiteralTypeInfo fieldInfo,
      GenericIndexed<String> stringDictionary,
      FixedIndexed<Long> longDictionary,
      FixedIndexed<Double> doubleDictionary,
      SmooshedFileMapper fileMapper
  )
  {
    this.metadata = metadata;
    this.columnConfig = columnConfig;
    this.rawColumn = rawColumn;
    this.nullValues = nullValues;
    this.fields = fields;
    this.fieldInfo = fieldInfo;
    this.stringDictionary = stringDictionary;
    this.longDictionary = longDictionary;
    this.doubleDictionary = doubleDictionary;
    this.fileMapper = fileMapper;
    this.closer = Closer.create();
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
    CloseableUtils.closeAndWrapExceptions(closer);
  }

  @Override
  public DimensionSelector makeDimensionSelector(String field, ReadableOffset readableOffset, ExtractionFn fn)
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      DictionaryEncodedColumn<?> col = (DictionaryEncodedColumn<?>) closer.register(getColumnHolder(field).getColumn());
      return col.makeDimensionSelector(readableOffset, fn);
    } else {
      return DimensionSelector.constant(null);
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String field, ReadableOffset readableOffset)
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      BaseColumn col = closer.register(getColumnHolder(field).getColumn());
      return col.makeColumnValueSelector(readableOffset);
    } else {
      return NilColumnValueSelector.instance();
    }
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      String field,
      ReadableVectorOffset readableOffset
  )
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      DictionaryEncodedColumn<?> col = (DictionaryEncodedColumn<?>) closer.register(getColumnHolder(field).getColumn());
      return col.makeSingleValueDimensionVectorSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  @Override
  public VectorObjectSelector makeVectorObjectSelector(String field, ReadableVectorOffset readableOffset)
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      BaseColumn col = closer.register(getColumnHolder(field).getColumn());
      return col.makeVectorObjectSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  @Override
  public VectorValueSelector makeVectorValueSelector(String field, ReadableVectorOffset readableOffset)
  {
    Preconditions.checkNotNull(field, "Null field");

    if (fields.indexOf(field) >= 0) {
      BaseColumn col = closer.register(getColumnHolder(field).getColumn());
      return col.makeVectorValueSelector(readableOffset);
    } else {
      return NilVectorSelector.create(readableOffset);
    }
  }

  @Nullable
  @Override
  public ColumnHolder readNestedFieldColumn(String field)
  {
    try {
      if (fields.indexOf(field) < 0) {
        return null;
      }
      final NestedLiteralTypeInfo.TypeSet types = fieldInfo.getTypes(fields.indexOf(field));
      final ByteBuffer dataBuffer = fileMapper.mapFile(
          NestedDataColumnSerializer.getFieldFileName(metadata.getFileNameBase(), field)
      );
      if (dataBuffer == null) {
        throw new ISE("Can't find field [%s] in [%s] file.", field, metadata.getFileNameBase());
      }

      ColumnBuilder columnBuilder = new ColumnBuilder().setFileMapper(fileMapper);
      // heh, maybe this should be its own class, or DictionaryEncodedColumnPartSerde could be cooler
      DictionaryEncodedColumnPartSerde.VERSION version = DictionaryEncodedColumnPartSerde.VERSION.fromByte(
          dataBuffer.get()
      );
      // we should check this someday soon, but for now just read it to push the buffer position ahead
      int flags = dataBuffer.getInt();
      Preconditions.checkState(
          flags == DictionaryEncodedColumnPartSerde.NO_FLAGS,
          StringUtils.format(
              "Unrecognized bits set in space reserved for future flags for field column [%s]",
              field
          )
      );

      final FixedIndexed<Integer> localDictionary = FixedIndexed.read(
          dataBuffer,
          NestedDataColumnSerializer.INT_TYPE_STRATEGY,
          metadata.getByteOrder(),
          Integer.BYTES
      );
      ByteBuffer bb = dataBuffer.asReadOnlyBuffer().order(metadata.getByteOrder());
      int longsLength = bb.getInt();
      int doublesLength = bb.getInt();
      dataBuffer.position(dataBuffer.position() + Integer.BYTES + Integer.BYTES);
      int pos = dataBuffer.position();
      final Supplier<ColumnarLongs> longs = longsLength > 0 ? CompressedColumnarLongsSupplier.fromByteBuffer(dataBuffer, metadata.getByteOrder()) : () -> null;
      dataBuffer.position(pos + longsLength);
      pos = dataBuffer.position();
      final Supplier<ColumnarDoubles> doubles = doublesLength > 0 ? CompressedColumnarDoublesSuppliers.fromByteBuffer(dataBuffer, metadata.getByteOrder()) : () -> null;
      dataBuffer.position(pos + doublesLength);
      final WritableSupplier<ColumnarInts> ints;
      if (version == DictionaryEncodedColumnPartSerde.VERSION.COMPRESSED) {
        ints = CompressedVSizeColumnarIntsSupplier.fromByteBuffer(dataBuffer, metadata.getByteOrder());
      } else {
        ints = VSizeColumnarInts.readFromByteBuffer(dataBuffer);
      }
      columnBuilder.setType(ValueType.STRING);

      GenericIndexed<ImmutableBitmap> rBitmaps = GenericIndexed.read(
          dataBuffer,
          metadata.getBitmapSerdeFactory().getObjectStrategy(),
          columnBuilder.getFileMapper()
      );
      Supplier<DictionaryEncodedColumn<?>> columnSupplier = () ->
          new NestedFieldLiteralDictionaryEncodedColumn(
              types,
              longs.get(),
              doubles.get(),
              ints.get(),
              stringDictionary,
              longDictionary,
              doubleDictionary,
              localDictionary,
              localDictionary.get(0) == 0
              ? rBitmaps.get(0)
              : metadata.getBitmapSerdeFactory().getBitmapFactory().makeEmptyImmutableBitmap()
          );
      columnBuilder.setHasMultipleValues(false)
             .setHasNulls(true)
             .setDictionaryEncodedColumnSupplier(columnSupplier);
      columnBuilder.setBitmapIndex(
          new NestedFieldLiteralBitmapIndexSupplier(
              types,
              metadata.getBitmapSerdeFactory().getBitmapFactory(),
              rBitmaps,
              localDictionary,
              stringDictionary,
              longDictionary,
              doubleDictionary
          )
      );
      return columnBuilder.build();
    }
    catch (IOException ex) {
      throw new RE(ex, "Failed to read data for [%s]", field);
    }
  }

  @Nullable
  @Override
  public BitmapIndex makeBitmapIndex(String field)
  {
    if (fields.indexOf(field) < 0) {
      return null;
    }
    return getColumnHolder(field).getBitmapIndex();
  }
}
