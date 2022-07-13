/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;

import java.util.List;
import java.util.Map;

/**
 * Slicer that handles multiple types of specs.
 */
public class MapInputSpecSlicer implements InputSpecSlicer
{
  private final Map<Class<? extends InputSpec>, InputSpecSlicer> splitterMap;

  public MapInputSpecSlicer(final Map<Class<? extends InputSpec>, InputSpecSlicer> splitterMap)
  {
    this.splitterMap = ImmutableMap.copyOf(splitterMap);
  }

  @Override
  public boolean canSliceDynamic(InputSpec inputSpec)
  {
    return getSlicer(inputSpec.getClass()).canSliceDynamic(inputSpec);
  }

  @Override
  public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
  {
    return getSlicer(inputSpec.getClass()).sliceStatic(inputSpec, maxNumSlices);
  }

  @Override
  public List<InputSlice> sliceDynamic(
      InputSpec inputSpec,
      int maxNumSlices,
      int maxFilesPerSlice,
      long maxBytesPerSlice
  )
  {
    return getSlicer(inputSpec.getClass()).sliceDynamic(inputSpec, maxNumSlices, maxFilesPerSlice, maxBytesPerSlice);
  }

  private InputSpecSlicer getSlicer(final Class<? extends InputSpec> clazz)
  {
    final InputSpecSlicer slicer = splitterMap.get(clazz);

    if (slicer == null) {
      throw new ISE("Cannot handle inputSpec of class [%s]", clazz.getName());
    }

    return slicer;
  }
}
