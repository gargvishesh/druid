/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterByKeyComparatorTest extends InitializedNullHandlingTest
{
  private static final RowSignature SIGNATURE =
      RowSignature.builder()
                  .add("1", ColumnType.LONG)
                  .add("2", ColumnType.STRING)
                  .add("3", ColumnType.LONG)
                  .add("4", ColumnType.DOUBLE)
                  .build();
  private static final Object[] OBJECTS1 = new Object[]{-1L, "foo", 2L, -1.2};
  private static final Object[] OBJECTS2 = new Object[]{-1L, null, 2L, 1.2d};
  private static final Object[] OBJECTS3 = new Object[]{-1L, "bar", 2L, 1.2d};
  private static final Object[] OBJECTS4 = new Object[]{-1L, "foo", 2L, 1.2d};
  private static final Object[] OBJECTS5 = new Object[]{-1L, "foo", 3L, 1.2d};
  private static final Object[] OBJECTS6 = new Object[]{-1L, "foo", 2L, 1.3d};
  private static final Object[] OBJECTS7 = new Object[]{1L, "foo", 2L, -1.2d};

  private static final List<Object[]> ALL_KEY_OBJECTS = Arrays.asList(
      OBJECTS1,
      OBJECTS2,
      OBJECTS3,
      OBJECTS4,
      OBJECTS5,
      OBJECTS6,
      OBJECTS7
  );

  @Test
  public void test_compare_AAAA() // AAAA = all ascending
  {
    final ClusterBy clusterBy = new ClusterBy(
        Arrays.asList(
            new ClusterByColumn("1", true),
            new ClusterByColumn("2", true),
            new ClusterByColumn("3", true),
            new ClusterByColumn("4", true)
        ),
        0
    );
    Assert.assertEquals(
        sortUsingObjectComparator(clusterBy, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(clusterBy, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_DDDD() // DDDD = all descending
  {
    final ClusterBy clusterBy = new ClusterBy(
        Arrays.asList(
            new ClusterByColumn("1", false),
            new ClusterByColumn("2", false),
            new ClusterByColumn("3", false),
            new ClusterByColumn("4", false)
        ),
        0
    );
    Assert.assertEquals(
        sortUsingObjectComparator(clusterBy, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(clusterBy, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_DAAD()
  {
    final ClusterBy clusterBy = new ClusterBy(
        Arrays.asList(
            new ClusterByColumn("1", false),
            new ClusterByColumn("2", true),
            new ClusterByColumn("3", true),
            new ClusterByColumn("4", false)
        ),
        0
    );
    Assert.assertEquals(
        sortUsingObjectComparator(clusterBy, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(clusterBy, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_ADDA()
  {
    final ClusterBy clusterBy = new ClusterBy(
        Arrays.asList(
            new ClusterByColumn("1", true),
            new ClusterByColumn("2", false),
            new ClusterByColumn("3", false),
            new ClusterByColumn("4", true)
        ),
        0
    );
    Assert.assertEquals(
        sortUsingObjectComparator(clusterBy, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(clusterBy, ALL_KEY_OBJECTS)
    );
  }

  @Test
  public void test_compare_DADA()
  {
    final ClusterBy clusterBy = new ClusterBy(
        Arrays.asList(
            new ClusterByColumn("1", true),
            new ClusterByColumn("2", false),
            new ClusterByColumn("3", true),
            new ClusterByColumn("4", false)
        ),
        0
    );
    Assert.assertEquals(
        sortUsingObjectComparator(clusterBy, ALL_KEY_OBJECTS),
        sortUsingKeyComparator(clusterBy, ALL_KEY_OBJECTS)
    );
  }

  private List<ClusterByKey> sortUsingKeyComparator(final ClusterBy clusterBy, final List<Object[]> objectss)
  {
    final List<ClusterByKey> sortedKeys = new ArrayList<>();

    for (final Object[] objects : objectss) {
      sortedKeys.add(ClusterByTestUtils.createKey(SIGNATURE, objects));
    }

    sortedKeys.sort(clusterBy.keyComparator());
    return sortedKeys;
  }

  private List<ClusterByKey> sortUsingObjectComparator(final ClusterBy clusterBy, final List<Object[]> objectss)
  {
    final List<Object[]> sortedObjectssCopy = objectss.stream().sorted(
        (o1, o2) -> {
          for (int i = 0; i < clusterBy.getColumns().size(); i++) {
            final ClusterByColumn sortColumn = clusterBy.getColumns().get(i);

            //noinspection unchecked, rawtypes
            final int cmp = Comparators.<Comparable>naturalNullsFirst()
                                       .compare((Comparable) o1[i], (Comparable) o2[i]);
            if (cmp != 0) {
              return sortColumn.descending() ? -cmp : cmp;
            }
          }

          return 0;
        }
    ).collect(Collectors.toList());

    final List<ClusterByKey> sortedKeys = new ArrayList<>();

    for (final Object[] objects : sortedObjectssCopy) {
      sortedKeys.add(ClusterByTestUtils.createKey(SIGNATURE, objects));
    }

    return sortedKeys;
  }
}
