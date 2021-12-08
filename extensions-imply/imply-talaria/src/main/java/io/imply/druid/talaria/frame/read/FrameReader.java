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
import org.apache.druid.segment.column.RowSignature;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Embeds the logic to read frames with a given {@link RowSignature}. Stateless and immutable, so it can be shared
 * between multiple frame readers as long as all frames have the same signature.
 */
public class FrameReader
{
  private final RowSignature signature;
  private final List<FrameColumnReader> columnReaders;

  private FrameReader(
      final RowSignature signature,
      final List<FrameColumnReader> columnReaders
  )
  {
    this.signature = signature;
    this.columnReaders = columnReaders;
  }

  public static FrameReader create(final RowSignature signature)
  {
    return new FrameReader(
        signature,
        signature.getColumnNames().stream().map(
            column -> {
              final int columnNumber = signature.indexOf(column);
              return FrameColumnReaders.create(
                  columnNumber,
                  Preconditions.checkNotNull(
                      signature.getColumnType(columnNumber).orElse(null),
                      "Type for column [%s]",
                      column
                  )
              );
            }
        ).collect(Collectors.toList())
    );
  }

  public RowSignature signature()
  {
    return signature;
  }

  public List<FrameColumnReader> columnReaders()
  {
    return columnReaders;
  }
}
