/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * TODO(gianm): migrate to {@link org.apache.druid.java.util.common.Either}
 */
public class Try<T>
{
  @Nullable
  private final T value;

  @Nullable
  private final Throwable error;

  private Try(@Nullable T value, @Nullable Throwable error)
  {
    this.value = value;
    this.error = error;
  }

  public static <T> Try<T> value(final T value)
  {
    return new Try<>(Preconditions.checkNotNull(value, "value"), null);
  }

  public static <T> Try<T> error(final Throwable error)
  {
    return new Try<>(null, Preconditions.checkNotNull(error, "error"));
  }

  public boolean isValue()
  {
    return value != null;
  }

  public boolean isError()
  {
    return error != null;
  }

  /**
   * If this Try represents a value, returns it. If this Try represents an error, throws it as a RuntimeException.
   *
   * If you want to be able to retrieve the error as-is, use {@link #isError()} and {@link #error()} instead.
   */
  public T getOrThrow()
  {
    if (error != null) {
      Throwables.propagateIfPossible(error);
      throw new RuntimeException(error);
    } else {
      return value;
    }
  }

  /**
   * Returns the error represented by this Try.
   *
   * @throws IllegalStateException if this Try does not represent an error
   */
  public Throwable error()
  {
    if (error == null) {
      // There was no error. So... throw an error instead of returning it.
      throw new ISE("No error");
    }

    return error;
  }

  /**
   * Applies a function to this value, if present.
   *
   * If the mapping function throws an exception, it is thrown by this method instead of being packed up into
   * the returned Try.
   *
   * If this Try represents an error, the mapping function is not applied.
   *
   * @throws NullPointerException if the mapping function returns null
   */
  public <R> Try<R> map(final Function<T, R> fn)
  {
    if (isValue()) {
      return Try.value(fn.apply(value));
    } else {
      // Safe because the value is never going to be returned.
      //noinspection unchecked
      return (Try<R>) this;
    }
  }
}
