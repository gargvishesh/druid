/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Closeable;
import java.util.function.Supplier;

@NotThreadSafe
public class LazyResourceHolder<T> implements ResourceHolder<T>
{
  private static final Logger log = new Logger(LazyResourceHolder.class);

  private final Supplier<Pair<T, Closeable>> supplier;
  private T resource = null;
  private Closeable closer = null;

  public LazyResourceHolder(final Supplier<Pair<T, Closeable>> supplier)
  {
    this.supplier = Preconditions.checkNotNull(supplier, "supplier");
  }

  @Override
  public T get()
  {
    if (resource == null) {
      final Pair<T, Closeable> supplied = supplier.get();
      resource = Preconditions.checkNotNull(supplied.lhs, "resource");
      closer = Preconditions.checkNotNull(supplied.rhs, "closer");
    }

    return resource;
  }

  @Override
  public void close()
  {
    if (resource != null) {
      try {
        closer.close();
      }
      catch (Throwable e) {
        log.noStackTrace().warn(e, "Exception encountered while closing resource: %s", resource);
      }
      finally {
        resource = null;
        closer = null;
      }
    }
  }
}
