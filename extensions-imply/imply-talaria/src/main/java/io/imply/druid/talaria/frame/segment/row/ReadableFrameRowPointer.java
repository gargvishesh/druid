/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.segment.row;

/**
 * Pointer to a row position in some memory. The row is expected to be in the row-based frame format. Only points to the beginning of the field, since all fields
 * can be read without knowing their entire length.
 */
public interface ReadableFrameRowPointer
{
  long position();

  long length();
}
