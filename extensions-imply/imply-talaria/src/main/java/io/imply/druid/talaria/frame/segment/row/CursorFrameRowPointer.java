/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment.row;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.FrameType;
import io.imply.druid.talaria.frame.write.RowBasedFrameWriter;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.segment.data.ReadableOffset;

public class CursorFrameRowPointer implements ReadableFrameRowPointer
{
  private final Frame frame;
  private final ReadableOffset offset;
  private final Memory rowPositions;

  int cachedOffset = -1;
  long cachedPosition;
  long cachedLength;

  public CursorFrameRowPointer(final Frame frame, final ReadableOffset offset)
  {
    this.frame = FrameType.ROW_BASED.ensureType(frame);
    this.offset = Preconditions.checkNotNull(offset, "offset");
    this.rowPositions = frame.region(RowBasedFrameWriter.ROW_OFFSET_REGION);
  }

  @Override
  public long position()
  {
    update();
    return cachedPosition;
  }

  @Override
  public long length()
  {
    update();
    return cachedLength;
  }

  private void update()
  {
    final int rowNumber = offset.getOffset();

    if (cachedOffset != rowNumber) {
      final long rowPosition;
      final int physicalRowNumber = frame.physicalRow(offset.getOffset());
      final long rowEndPosition = rowPositions.getLong((long) Long.BYTES * physicalRowNumber);

      if (physicalRowNumber == 0) {
        rowPosition = 0;
      } else {
        rowPosition = rowPositions.getLong((long) Long.BYTES * (physicalRowNumber - 1));
      }

      cachedOffset = rowNumber;
      cachedPosition = rowPosition;
      cachedLength = rowEndPosition - rowPosition;
    }
  }
}
