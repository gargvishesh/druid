/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.read;

import org.apache.druid.segment.column.RowSignature;

/**
 * Embeds the logic to read a specific column from frames with a specific {@link RowSignature}. Stateless and immutable,
 * so it can be shared between multiple frame readers as long as all frames have the same signature.
 *
 * Generally this class is retrieved from a {@link FrameReader}.
 */
public interface FrameColumnReader
{
  /**
   * Returns a column reference for the provided frame.
   */
  ColumnPlus readColumn(Frame frame);
}
