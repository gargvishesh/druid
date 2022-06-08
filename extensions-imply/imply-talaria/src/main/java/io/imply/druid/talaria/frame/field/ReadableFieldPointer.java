/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

/**
 * Pointer to a field position in some memory. Only points to the beginning of the field, since all fields
 * can be read without knowing their entire length.
 *
 * See {@link io.imply.druid.talaria.frame.write.RowBasedFrameWriter} for details about the format.
 */
public interface ReadableFieldPointer
{
  /**
   * Starting position of the field.
   */
  long position();
}
