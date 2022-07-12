/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel.controller;

import io.imply.druid.talaria.input.InputSlice;
import io.imply.druid.talaria.input.InputSpec;
import io.imply.druid.talaria.input.InputSpecSlicer;

import java.util.ArrayList;
import java.util.List;

public class ControllerTestInputSpecSlicer implements InputSpecSlicer
{
  @Override
  public boolean canSliceDynamic(InputSpec inputSpec)
  {
    return false;
  }

  @Override
  public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
  {
    final List<InputSlice> slices = new ArrayList<>(maxNumSlices);
    for (int i = 0; i < maxNumSlices; i++) {
      slices.add(new ControllerTestInputSlice());
    }
    return slices;
  }

  @Override
  public List<InputSlice> sliceDynamic(
      InputSpec inputSpec,
      int maxNumSlices,
      int maxFilesPerSlice,
      long maxBytesPerSlice
  )
  {
    throw new UnsupportedOperationException();
  }
}
