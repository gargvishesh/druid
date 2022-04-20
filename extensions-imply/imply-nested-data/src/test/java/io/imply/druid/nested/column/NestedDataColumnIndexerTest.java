/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class NestedDataColumnIndexerTest extends InitializedNullHandlingTest
{
  @Test
  public void testSomeThings()
  {
    NestedDataColumnIndexer indexer = new NestedDataColumnIndexer();
    Assert.assertEquals(0, indexer.getCardinality());

    EncodedKeyComponent<StructuredData> key;
    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", "foo"), false);
    Assert.assertEquals(230, key.getEffectiveSizeBytes());
    Assert.assertEquals(1, indexer.getCardinality());
    // adding same value only adds estimated size of value itself
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", "foo"), false);
    Assert.assertEquals(112, key.getEffectiveSizeBytes());
    Assert.assertEquals(1, indexer.getCardinality());
    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(10L, false);
    Assert.assertEquals(94, key.getEffectiveSizeBytes());
    Assert.assertEquals(2, indexer.getCardinality());
    // adding same value only adds estimated size of value itself
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(10L, false);
    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(2, indexer.getCardinality());
    // new raw value, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(11L, false);
    Assert.assertEquals(48, key.getEffectiveSizeBytes());
    Assert.assertEquals(3, indexer.getCardinality());

    // new raw value, new fields
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 10L), false);
    Assert.assertEquals(276, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value, re-use fields and dictionary
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableList.of(1L, 2L, 10L), false);
    Assert.assertEquals(56, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value, new fields
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", ImmutableList.of(1L, 2L, 10L)), false);
    Assert.assertEquals(292, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(ImmutableMap.of("x", ImmutableList.of(1L, 2L, 10L)), false);
    Assert.assertEquals(118, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
  }
}
