/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.write;

import org.apache.druid.segment.ColumnSelectorFactory;

/**
 * Interface for creating {@link FrameWriter}.
 */
public interface FrameWriterFactory
{
  /**
   * Create a writer where {@link FrameWriter#addSelection()} adds the current row from a {@link ColumnSelectorFactory}.
   */
  FrameWriter newFrameWriter(ColumnSelectorFactory columnSelectorFactory);

  long allocatorCapacity();
}
