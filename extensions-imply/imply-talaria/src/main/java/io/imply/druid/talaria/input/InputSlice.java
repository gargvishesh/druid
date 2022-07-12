/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Slice of an {@link InputSpec} assigned to a particular worker.
 *
 * On the controller, these are produced using {@link InputSpecSlicer}. On workers, these are read
 * using {@link InputSliceReader}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface InputSlice
{
  /**
   * Returns the number of files contained within this split. This is the same number that would be added to
   * {@link io.imply.druid.talaria.counters.CounterTracker} on full iteration through {@link InputSliceReader#attach}.
   *
   * May be zero for some kinds of slices, even if they contain data, if the input is not file-based.
   */
  int numFiles();
}
