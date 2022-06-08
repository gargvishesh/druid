/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment.row;

import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.MemoryRange;
import io.imply.druid.talaria.frame.field.FieldReader;
import io.imply.druid.talaria.frame.field.RowMemoryFieldPointer;
import io.imply.druid.talaria.frame.write.FrameWriterUtils;
import io.imply.druid.talaria.frame.write.RowBasedFrameWriter;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.List;

public class FrameColumnSelectorFactory implements ColumnSelectorFactory, RowIdSupplier
{
  /**
   * Name of the virtual column that contains the {@link RowSignature} of frames from this
   * {@link ColumnSelectorFactory}. This is necessary because callers need it to verify that {@link #ROW_MEMORY_COLUMN}
   * is usable, but the interface does not provide a natural way to retrieve the underlying signature.
   *
   * Guaranteed not to appear in the frame itself due to {@link FrameWriterUtils#findDisallowedFieldNames} checks.
   */
  public static final String ROW_SIGNATURE_COLUMN = FrameWriterUtils.RESERVED_FIELD_PREFIX + "_frame_row_signature";

  /**
   * Name of the virtual column that contains {@link MemoryRange} for direct access to row memory.
   *
   * Guaranteed not to appear in the frame itself due to {@link FrameWriterUtils#findDisallowedFieldNames} checks.
   */
  public static final String ROW_MEMORY_COLUMN = FrameWriterUtils.RESERVED_FIELD_PREFIX + "_frame_row_mem";

  private final Memory dataRegion;
  private final RowSignature frameSignature;
  private final List<FieldReader> fieldReaders;
  private final ReadableFrameRowPointer rowPointer;

  public FrameColumnSelectorFactory(
      final Frame frame,
      final RowSignature frameSignature,
      final List<FieldReader> fieldReaders,
      final ReadableFrameRowPointer rowPointer
  )
  {
    this.dataRegion = FrameType.ROW_BASED.ensureType(frame).region(RowBasedFrameWriter.ROW_DATA_REGION);
    this.frameSignature = frameSignature;
    this.fieldReaders = fieldReaders;
    this.rowPointer = rowPointer;
  }

  @Override
  public DimensionSelector makeDimensionSelector(final DimensionSpec dimensionSpec)
  {
    return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(final String columnName)
  {
    if (ROW_SIGNATURE_COLUMN.equals(columnName)) {
      return new RowSignatureSelector(frameSignature);
    } else if (ROW_MEMORY_COLUMN.equals(columnName)) {
      return new RowMemorySelector(dataRegion, rowPointer);
    } else {
      final int columnNumber = frameSignature.indexOf(columnName);

      if (columnNumber < 0) {
        return NilColumnValueSelector.instance();
      } else {
        final RowMemoryFieldPointer fieldPointer =
            new RowMemoryFieldPointer(dataRegion, rowPointer, columnNumber, fieldReaders.size());
        return fieldReaders.get(columnNumber).makeColumnValueSelector(dataRegion, fieldPointer);
      }
    }
  }

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return this;
  }

  @Override
  public long getRowId()
  {
    return rowPointer.position();
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(final String column)
  {
    if (ROW_SIGNATURE_COLUMN.equals(column) || ROW_MEMORY_COLUMN.equals(column)) {
      // OK to use UNKNOWN_COMPLEX because we are not serializing these columns.
      return ColumnCapabilitiesImpl.createDefault().setType(ColumnType.UNKNOWN_COMPLEX);
    } else {
      return frameSignature.getColumnCapabilities(column);
    }
  }

  private DimensionSelector makeDimensionSelectorUndecorated(final DimensionSpec dimensionSpec)
  {
    final int columnNumber = frameSignature.indexOf(dimensionSpec.getDimension());

    if (columnNumber < 0) {
      return DimensionSelector.constant(null, dimensionSpec.getExtractionFn());
    } else {
      final RowMemoryFieldPointer fieldPointer =
          new RowMemoryFieldPointer(dataRegion, rowPointer, columnNumber, fieldReaders.size());
      return fieldReaders.get(columnNumber)
                         .makeDimensionSelector(dataRegion, fieldPointer, dimensionSpec.getExtractionFn());
    }
  }

  private static class RowMemorySelector extends ObjectColumnSelector<MemoryRange<Memory>>
  {
    private final Memory frameDataRegion;
    private final ReadableFrameRowPointer rowPointer;
    private final MemoryRange<Memory> retVal;

    public RowMemorySelector(
        final Memory frameDataRegion,
        final ReadableFrameRowPointer rowPointer
    )
    {
      this.frameDataRegion = frameDataRegion;
      this.rowPointer = rowPointer;
      this.retVal = new MemoryRange<>(frameDataRegion, 0, 0);
    }

    @Nullable
    @Override
    public MemoryRange<Memory> getObject()
    {
      retVal.set(frameDataRegion, rowPointer.position(), rowPointer.length());
      return retVal;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public Class classOfObject()
    {
      return MemoryRange.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }
  }

  private static class RowSignatureSelector extends ObjectColumnSelector<RowSignature>
  {
    private final RowSignature signature;

    public RowSignatureSelector(final RowSignature signature)
    {
      this.signature = signature;
    }

    @Override
    public RowSignature getObject()
    {
      return signature;
    }

    @Override
    public Class<? extends RowSignature> classOfObject()
    {
      return RowSignature.class;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      // Do nothing.
    }
  }
}
