/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.timeseries.utils;

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;

import java.util.Collection;

public class ImplyDoubleArrayList extends DoubleArrayList
{
  public ImplyDoubleArrayList()
  {
    super();
  }

  public ImplyDoubleArrayList(int capacity)
  {
    super(capacity);
  }

  public ImplyDoubleArrayList(double[] a)
  {
    super(a);
  }

  public ImplyDoubleArrayList(Collection<? extends Double> c)
  {
    super(c);
  }

  public double[] getDoubleArray()
  {
    return a;
  }
}
