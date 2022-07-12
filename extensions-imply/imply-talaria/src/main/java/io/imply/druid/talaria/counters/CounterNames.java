/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.counters;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.ordering.StringComparators;

import java.util.Comparator;
import java.util.Map;

/**
 * Standard names for counters.
 */
public class CounterNames
{
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String SORT = "sort";
  private static final String SORT_PROGRESS = "sortProgress";
  private static final String WARNINGS = "warnings";
  private static final Comparator<String> COMPARATOR = new NameComparator();

  private CounterNames()
  {
    // No construction: statics only.
  }

  /**
   * Standard name for an input channel counter created by {@link CounterTracker#channel}.
   */
  public static String inputChannel(final int inputNumber)
  {
    return StringUtils.format("%s%d", INPUT, inputNumber);
  }

  /**
   * Standard name for an output channel counter created by {@link CounterTracker#channel}.
   */
  public static String outputChannel()
  {
    return OUTPUT;
  }

  /**
   * Standard name for a sort channel counter created by {@link CounterTracker#channel}.
   */
  public static String sortChannel()
  {
    return SORT;
  }

  /**
   * Standard name for a sort progress counter created by {@link CounterTracker#sortProgress()}.
   */
  public static String sortProgress()
  {
    return SORT_PROGRESS;
  }

  /**
   * Standard name for a warnings counter created by {@link CounterTracker#warnings()}.
   */
  public static String warnings()
  {
    return WARNINGS;
  }

  /**
   * Standard comparator for counter names. Not necessary for functionality, but helps with human-readability.
   */
  public static Comparator<String> comparator()
  {
    return COMPARATOR;
  }

  /**
   * Comparator that ensures counters are sorted in a nice order when serialized to JSON. Not necessary for
   * functionality, but helps with human-readability.
   */
  private static class NameComparator implements Comparator<String>
  {
    private static final Map<String, Integer> ORDER =
        ImmutableMap.<String, Integer>builder()
                    .put(OUTPUT, 0)
                    .put(SORT, 1)
                    .put(SORT_PROGRESS, 2)
                    .put(WARNINGS, 3)
                    .build();

    @Override
    public int compare(final String name1, final String name2)
    {
      final boolean isInput1 = name1.startsWith(INPUT);
      final boolean isInput2 = name2.startsWith(INPUT);

      if (isInput1 && isInput2) {
        // Compare INPUT alphanumerically, so e.g. "input2" is before "input10"
        return StringComparators.ALPHANUMERIC.compare(name1, name2);
      } else if (isInput1 != isInput2) {
        // INPUT goes first
        return isInput1 ? -1 : 1;
      }

      assert !isInput1 && !isInput2;

      final Integer order1 = ORDER.get(name1);
      final Integer order2 = ORDER.get(name2);

      if (order1 != null && order2 != null) {
        // Respect ordering from ORDER
        return Integer.compare(order1, order2);
      } else if (order1 != null) {
        // Names from ORDER go before names that are not in ORDER
        return -1;
      } else if (order2 != null) {
        // Names from ORDER go before names that are not in ORDER
        return 1;
      } else {
        assert order1 == null && order2 == null;
        return name1.compareTo(name2);
      }
    }
  }
}
