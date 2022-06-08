/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import io.imply.druid.talaria.frame.segment.row.ConstantFrameRowPointer;
import org.apache.datasketches.memory.Memory;

import java.util.ArrayList;
import java.util.List;

/**
 * Class for reading rows in the same format as used by {@link io.imply.druid.talaria.frame.FrameType#ROW_BASED}.
 *
 * Stateless and immutable.
 */
public class RowReader
{
  private final List<FieldReader> fieldReaders;

  public RowReader(List<FieldReader> fieldReaders)
  {
    this.fieldReaders = fieldReaders;
  }

  public FieldReader fieldReader(final int fieldNumber)
  {
    return fieldReaders.get(fieldNumber);
  }

  public int fieldCount()
  {
    return fieldReaders.size();
  }

  /**
   * Read a particular field value as an object.
   *
   * For performance reasons, prefer {@link io.imply.druid.talaria.frame.read.FrameReader#makeCursorFactory}
   * for reading many rows out of a frame.
   */
  public Object readField(final Memory memory, final long rowPosition, final long rowLength, final int fieldNumber)
  {
    return fieldReaders.get(fieldNumber)
                       .makeColumnValueSelector(
                           memory,
                           new RowMemoryFieldPointer(
                               memory,
                               new ConstantFrameRowPointer(rowPosition, rowLength),
                               fieldNumber,
                               fieldReaders.size()
                           )
                       ).getObject();
  }

  /**
   * Read an entire row as a list of objects.
   *
   * For performance reasons, prefer {@link io.imply.druid.talaria.frame.read.FrameReader#makeCursorFactory}
   * for reading many rows out of a frame.
   */
  public List<Object> readRow(final Memory memory, final long rowPosition, final long rowLength)
  {
    final List<Object> objects = new ArrayList<>(fieldReaders.size());

    for (int i = 0; i < fieldReaders.size(); i++) {
      objects.add(readField(memory, rowPosition, rowLength, i));
    }

    return objects;
  }
}
