/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.util;

import com.google.common.base.Preconditions;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

/**
 * An Iterator that returns a single element from a {@link Supplier}.
 */
public class SupplierIterator<T> implements Iterator<T>
{
  private Supplier<T> supplier;

  public SupplierIterator(final Supplier<T> supplier)
  {
    this.supplier = Preconditions.checkNotNull(supplier, "supplier");
  }

  @Override
  public boolean hasNext()
  {
    return supplier != null;
  }

  @Override
  public T next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final T thing = supplier.get();
    supplier = null;
    return thing;
  }
}
