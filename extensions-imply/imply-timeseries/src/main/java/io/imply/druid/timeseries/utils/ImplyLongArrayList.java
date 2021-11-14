/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.utils;

import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.Collection;
import java.util.List;

public class ImplyLongArrayList extends LongArrayList
{
  public ImplyLongArrayList()
  {
    super();
  }
  
  public ImplyLongArrayList(int capacity)
  {
    super(capacity);  
  }

  public ImplyLongArrayList(long[] a)
  {
    super(a);
  }

  public ImplyLongArrayList(Collection<? extends Long> c)
  {
    super(c);
  }

  public ImplyLongArrayList(List<Number> list)
  {
    super(list.size());
    for (Number data : list) {
      add(data.longValue());
    }
  }
  
  public long[] getLongArray()
  {
    return a;
  }
}
