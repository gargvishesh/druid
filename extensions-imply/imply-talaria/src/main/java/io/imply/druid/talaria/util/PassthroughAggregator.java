/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;

public class PassthroughAggregator implements Aggregator
{
  private final BaseObjectColumnValueSelector<?> selector;
  private boolean didSet = false;
  private Object val;

  public PassthroughAggregator(final BaseObjectColumnValueSelector<?> selector)
  {
    this.selector = selector;
  }

  @Override
  public void aggregate()
  {
    if (didSet) {
      throw new ISE("Cannot set twice");
    }

    val = selector.getObject();
    didSet = true;
  }

  @Nullable
  @Override
  public Object get()
  {
    if (!didSet) {
      throw new ISE("Cannot call get() before aggregate()");
    }

    return val;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLong()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close()
  {
    // Nothing to do.
  }
}
