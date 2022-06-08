/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;

import javax.annotation.Nullable;

/**
 * Embeds the logic to read a specific field from row-based frames or from {@link ClusterByKey}.
 *
 * Most callers should use {@link io.imply.druid.talaria.frame.read.FrameReader} or
 * {@link io.imply.druid.talaria.frame.cluster.ClusterByKeyReader} rather than using this interface directly.
 *
 * Stateless and immutable.
 */
public interface FieldReader
{
  /**
   * Create a {@link ColumnValueSelector} backed by some memory and a moveable pointer.
   */
  ColumnValueSelector<?> makeColumnValueSelector(Memory memory, ReadableFieldPointer fieldPointer);

  /**
   * Create a {@link DimensionSelector} backed by some memory and a moveable pointer.
   */
  DimensionSelector makeDimensionSelector(
      Memory memory,
      ReadableFieldPointer fieldPointer,
      @Nullable ExtractionFn extractionFn
  );

  /**
   * Whether this field is comparable. Comparable fields can be compared as unsigned bytes.
   */
  boolean isComparable();
}
