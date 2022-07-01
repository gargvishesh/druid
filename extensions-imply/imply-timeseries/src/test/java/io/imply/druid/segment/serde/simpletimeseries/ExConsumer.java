/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import java.util.Objects;

/**
 * copied java.util.function.Consumer and added exeception
 * @param <T>
 */
public interface ExConsumer<T, E extends Throwable>
{
  void accept(T t) throws E;

  default ExConsumer<T, E> andThen(ExConsumer<? super T, E> after)
  {
    Objects.requireNonNull(after);
    return (T t) -> {
      accept(t);
      after.accept(t);
    };
  }
}
