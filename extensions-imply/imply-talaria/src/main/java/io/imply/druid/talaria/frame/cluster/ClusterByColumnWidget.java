/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import org.apache.druid.segment.ColumnSelectorFactory;

import java.util.Comparator;
import java.util.function.Supplier;

/**
 * Tooling necessary to work with a particular {@link ClusterByColumn}. Use {@link ClusterByColumnWidgets#create} to
 * create instances of this interface.
 *
 * Behavior of these widgets must be consistent with the same-typed
 * {@link io.imply.druid.talaria.frame.write.FrameColumnWriter#compare}.
 */
public interface ClusterByColumnWidget<T, SelectorType>
{
  /**
   * Given a {@link ColumnSelectorFactory}, creates a selector that can be passed into {@link #reader}.
   */
  SelectorType makeSelector(ColumnSelectorFactory columnSelectorFactory);

  /**
   * Given a selector, provides a supplier that reads the current comparable object out of the selector. The returned
   * objects can be compared by {@link #objectComparator()}.
   */
  Supplier<T> reader(SelectorType selector);

  /**
   * Returns a comparator that can compare the objects supplied by {@link #reader}.
   */
  Comparator<T> objectComparator();
}
