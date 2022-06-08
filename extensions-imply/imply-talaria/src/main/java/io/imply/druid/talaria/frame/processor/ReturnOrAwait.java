/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Objects;

public class ReturnOrAwait<T>
{
  private static final IntSet RANGE_SET_ONE = IntSets.singleton(0);

  @Nullable
  private final T retVal;

  @Nullable
  private final IntSet await;

  private final boolean awaitAll;

  private ReturnOrAwait(@Nullable T retVal, @Nullable IntSet await, final boolean awaitAll)
  {
    this.retVal = retVal;
    this.await = await;
    this.awaitAll = awaitAll;

    if (retVal != null && await != null) {
      throw new IAE("Cannot have a value when await != null");
    }
  }

  public static <T> ReturnOrAwait<T> runAgain()
  {
    return new ReturnOrAwait<>(null, IntSets.emptySet(), true);
  }

  /**
   * Wait for all of the provided channels to become readable. It is OK to pass in a mutable set.
   */
  public static <T> ReturnOrAwait<T> awaitAll(final IntSet await)
  {
    return new ReturnOrAwait<>(null, await, true);
  }

  public static <T> ReturnOrAwait<T> awaitAll(final int count)
  {
    return new ReturnOrAwait<>(null, rangeSet(count), true);
  }

  /**
   * Wait for any of the provided channels to become readable. It is OK to pass in a mutable set.
   */
  public static <T> ReturnOrAwait<T> awaitAny(final IntSet await)
  {
    return new ReturnOrAwait<>(null, await, false);
  }

  public static <T> ReturnOrAwait<T> returnObject(final T o)
  {
    return new ReturnOrAwait<>(o, null, false);
  }

  @Nullable
  public T value()
  {
    if (isContinue()) {
      throw new ISE("No value yet");
    }

    return retVal;
  }

  public IntSet awaitSet()
  {
    if (isReturn()) {
      throw new ISE("No await set");
    }

    return await;
  }

  public boolean isAwaitAll()
  {
    return awaitAll;
  }

  public boolean isReturn()
  {
    return await == null;
  }

  public boolean isContinue()
  {
    return await != null;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReturnOrAwait<?> that = (ReturnOrAwait<?>) o;
    return awaitAll == that.awaitAll && Objects.equals(retVal, that.retVal) && Objects.equals(
        await,
        that.await
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(retVal, await, awaitAll);
  }

  @Override
  public String toString()
  {
    if (isContinue()) {
      return "await=" + await + (awaitAll ? "(all)" : "(any)");
    } else {
      return "return=" + retVal;
    }
  }

  private static IntSet rangeSet(final int count)
  {
    if (count == 0) {
      return IntSets.emptySet();
    } else if (count == 1) {
      return RANGE_SET_ONE;
    } else {
      final IntSet retVal = new IntOpenHashSet();

      for (int i = 0; i < count; i++) {
        retVal.add(i);
      }

      return retVal;
    }
  }
}
