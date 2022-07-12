/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import java.util.List;

/**
 * Slices {@link InputSpec} into {@link InputSlice} on the controller.
 */
public interface InputSpecSlicer
{
  boolean canSliceDynamic(InputSpec inputSpec);

  /**
   * Slice a spec into a given maximum number of slices. The returned list may contain fewer slices, but cannot
   * contain more.
   *
   * This method creates as many slices as possible while staying at or under maxNumSlices. For example, if a spec
   * contains 8 files, and maxNumSlices is 10, then 8 slices will be created.
   */
  List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices);

  /**
   * Slice a spec based on a particular maximum number of files and bytes per slice.
   *
   * This method creates as few slices as possible, while keeping each slice under the provided limits.
   */
  List<InputSlice> sliceDynamic(InputSpec inputSpec, int maxNumSlices, int maxFilesPerSlice, long maxBytesPerSlice);
}
