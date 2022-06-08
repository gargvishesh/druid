/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment.columnar;

import io.imply.druid.talaria.frame.Frame;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.vector.VectorOffset;

import java.io.IOException;

/**
 * A {@link VectorCursor} that is based on a {@link Frame}.
 *
 * This class is only used for columnar frames. It is not used for row-based frames.
 */
@SuppressWarnings("unused") // TODO(gianm): implement this
public class FrameVectorCursor implements VectorCursor
{
  private final VectorOffset offset;
  private final VectorColumnSelectorFactory columnSelectorFactory;
  private final Closer closer;

  FrameVectorCursor(
      final Frame frame,
      final VectorOffset offset,
      final VectorColumnSelectorFactory columnSelectorFactory,
      final Closer closer
  )
  {
    this.offset = offset;
    this.closer = Closer.create();
    this.columnSelectorFactory = columnSelectorFactory;
  }

  @Override
  public VectorColumnSelectorFactory getColumnSelectorFactory()
  {
    return columnSelectorFactory;
  }

  @Override
  public void advance()
  {
    offset.advance();
    BaseQuery.checkInterrupted();
  }

  @Override
  public boolean isDone()
  {
    return offset.isDone();
  }

  @Override
  public void reset()
  {
    offset.reset();
  }

  @Override
  public void close()
  {
    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getMaxVectorSize()
  {
    return offset.getMaxVectorSize();
  }

  @Override
  public int getCurrentVectorSize()
  {
    return offset.getCurrentVectorSize();
  }
}
