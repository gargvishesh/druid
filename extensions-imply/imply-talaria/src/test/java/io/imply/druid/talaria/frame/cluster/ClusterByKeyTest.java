/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class ClusterByKeyTest extends InitializedNullHandlingTest
{
  @Test
  public void testEqualsAndHashCode()
  {
    // Can't use EqualsVerifier because it requires that cached hashcodes be computed in the constructor.
    final RowSignature signatureLong = RowSignature.builder().add("1", ColumnType.LONG).build();
    final RowSignature signatureLongString =
        RowSignature.builder().add("1", ColumnType.LONG).add("2", ColumnType.STRING).build();

    //noinspection AssertBetweenInconvertibleTypes: testing this case on purpose
    Assert.assertNotEquals(
        "not a key",
        ClusterByTestUtils.createKey(signatureLong, 1L)
    );

    Assert.assertEquals(
        ClusterByTestUtils.createKey(signatureLong, 1L),
        ClusterByTestUtils.createKey(signatureLong, 1L)
    );

    Assert.assertNotEquals(
        ClusterByTestUtils.createKey(signatureLong, 1L),
        ClusterByTestUtils.createKey(signatureLong, 2L)
    );

    Assert.assertEquals(
        ClusterByTestUtils.createKey(signatureLongString, 1L, "abc"),
        ClusterByTestUtils.createKey(signatureLongString, 1L, "abc")
    );

    Assert.assertNotEquals(
        ClusterByTestUtils.createKey(signatureLongString, 1L, "abc"),
        ClusterByTestUtils.createKey(signatureLongString, 1L, "def")
    );

    Assert.assertEquals(
        ClusterByTestUtils.createKey(signatureLong, 1L).hashCode(),
        ClusterByTestUtils.createKey(signatureLong, 1L).hashCode()
    );

    Assert.assertNotEquals(
        ClusterByTestUtils.createKey(signatureLong, 1L).hashCode(),
        ClusterByTestUtils.createKey(signatureLong, 2L).hashCode()
    );

    Assert.assertEquals(
        ClusterByTestUtils.createKey(signatureLongString, 1L, "abc").hashCode(),
        ClusterByTestUtils.createKey(signatureLongString, 1L, "abc").hashCode()
    );

    Assert.assertNotEquals(
        ClusterByTestUtils.createKey(signatureLongString, 1L, "abc").hashCode(),
        ClusterByTestUtils.createKey(signatureLongString, 1L, "def").hashCode()
    );
  }
}
