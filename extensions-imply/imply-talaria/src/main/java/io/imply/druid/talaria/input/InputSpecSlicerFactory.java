/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import io.imply.druid.talaria.kernel.ReadablePartitions;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

/**
 * Creates an {@link InputSpecSlicer} given a map of stage numbers to output partitions of that stage.
 *
 * In production, this is typically used to create a {@link MapInputSpecSlicer} that contains a
 * {@link StageInputSpecSlicer}. The stage slicer needs the output partitions map in order to do its job, and
 * these aren't always known until the stage has started.
 */
public interface InputSpecSlicerFactory
{
  InputSpecSlicer makeSlicer(Int2ObjectMap<ReadablePartitions> stagePartitionsMap);
}
