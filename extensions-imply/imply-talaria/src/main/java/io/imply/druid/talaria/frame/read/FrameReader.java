/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.cluster.ClusterByColumn;
import io.imply.druid.talaria.frame.field.FieldReader;
import io.imply.druid.talaria.frame.field.FieldReaders;
import io.imply.druid.talaria.frame.read.columnar.FrameColumnReader;
import io.imply.druid.talaria.frame.read.columnar.FrameColumnReaders;
import io.imply.druid.talaria.frame.segment.row.FrameCursorFactory;
import io.imply.druid.talaria.frame.write.FrameWriterUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Embeds the logic to read frames with a given {@link RowSignature}.
 *
 * Stateless and immutable.
 */
public class FrameReader
{
  private final RowSignature signature;

  // Column readers, for columnar frames.
  private final List<FrameColumnReader> columnReaders;

  // Field readers, for row-based frames.
  private final List<FieldReader> fieldReaders;

  private FrameReader(
      final RowSignature signature,
      final List<FrameColumnReader> columnReaders,
      final List<FieldReader> fieldReaders
  )
  {
    this.signature = signature;
    this.columnReaders = columnReaders;
    this.fieldReaders = fieldReaders;
  }

  /**
   * Create a reader for frames with a given {@link RowSignature}. The signature must exactly match the frames to be
   * read, or else behavior is undefined.
   */
  public static FrameReader create(final RowSignature signature)
  {
    // Double-check that the frame does not have any disallowed field names. Generally, we expect this to be
    // caught on the write side, but we do it again here for safety.
    final Set<String> disallowedFieldNames = FrameWriterUtils.findDisallowedFieldNames(signature);
    if (!disallowedFieldNames.isEmpty()) {
      throw new IAE("Disallowed field names: %s", disallowedFieldNames);
    }

    final List<FrameColumnReader> columnReaders = new ArrayList<>(signature.size());
    final List<FieldReader> fieldReaders = new ArrayList<>(signature.size());

    for (int columnNumber = 0; columnNumber < signature.size(); columnNumber++) {
      final ColumnType columnType =
          Preconditions.checkNotNull(
              signature.getColumnType(columnNumber).orElse(null),
              "Type for column [%s]",
              signature.getColumnName(columnNumber)
          );

      columnReaders.add(FrameColumnReaders.create(columnNumber, columnType));
      fieldReaders.add(FieldReaders.create(columnType));
    }

    return new FrameReader(signature, columnReaders, fieldReaders);
  }

  public RowSignature signature()
  {
    return signature;
  }

  /**
   * Returns capabilities for a particular column in a particular frame.
   *
   * Preferred over {@link RowSignature#getColumnCapabilities(String)} when reading a particular frame, because this
   * method has more insight into what's actually going on with that specific frame (nulls, multivalue, etc). The
   * RowSignature version is based solely on type.
   */
  @Nullable
  public ColumnCapabilities columnCapabilities(final Frame frame, final String columnName)
  {
    final int columnNumber = signature.indexOf(columnName);

    if (columnNumber < 0) {
      return null;
    } else {
      // Better than frameReader.frameSignature().getColumnCapabilities(column), because this method has more
      // insight into what's actually going on with this column (nulls, multivalue, etc).
      return columnReaders.get(columnNumber).readColumn(frame).getCapabilities();
    }
  }

  /**
   * Create a {@link CursorFactory} for the given frame.
   */
  public CursorFactory makeCursorFactory(final Frame frame)
  {
    switch (frame.type()) {
      case COLUMNAR:
        return new io.imply.druid.talaria.frame.segment.columnar.FrameCursorFactory(frame, signature, columnReaders);
      case ROW_BASED:
        return new FrameCursorFactory(frame, this, fieldReaders);
      default:
        throw new ISE("Unrecognized frame type [%s]", frame.type());
    }
  }

  /**
   * Create a {@link FrameComparisonWidget} for the given frame.
   *
   * Only possible for frames of type {@link io.imply.druid.talaria.frame.FrameType#ROW_BASED}. The provided
   * sortColumns must be a prefix of {@link #signature()}.
   */
  public FrameComparisonWidget makeComparisonWidget(final Frame frame, final List<ClusterByColumn> sortColumns)
  {
    if (!FrameWriterUtils.areSortColumnsPrefixOfSignature(signature, sortColumns)) {
      throw new IAE("Sort columns must be a prefix of the signature");
    }

    // Verify that all sort columns are comparable.
    for (final ClusterByColumn sortColumn : sortColumns) {
      if (!fieldReaders.get(signature.indexOf(sortColumn.columnName())).isComparable()) {
        throw new IAE(
            "Sort column [%s] is not comparable (type = [%s])",
            sortColumn.columnName(),
            signature.getColumnType(sortColumn.columnName()).orElse(null)
        );
      }
    }

    return FrameComparisonWidgetImpl.create(frame, this, sortColumns);
  }
}
