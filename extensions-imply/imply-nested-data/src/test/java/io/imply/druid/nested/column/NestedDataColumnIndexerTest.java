/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.EncodedKeyComponent;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class NestedDataColumnIndexerTest extends InitializedNullHandlingTest
{
  @Test
  public void testSomeThingsNestedIndexer()
  {
    NestedDataColumnIndexer indexer = new NestedDataColumnIndexer();
    testIndexer(indexer);
  }

  @Test
  public void testSomeThingsJsonIndexer()
  {
    JsonColumnIndexer indexer = new JsonColumnIndexer();
    testIndexer(indexer);
  }

  @Test
  public void testJsonIndexerStrings() throws JsonProcessingException
  {
    // same checks as testIndexer but json strings
    JsonColumnIndexer indexer = new JsonColumnIndexer();
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Assert.assertEquals(0, indexer.getCardinality());

    EncodedKeyComponent<StructuredData> key;
    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        jsonMapper.writeValueAsString(ImmutableMap.of("x", "foo")),
        false
    );
    Assert.assertEquals(230, key.getEffectiveSizeBytes());
    Assert.assertEquals(1, indexer.getCardinality());

    // adding same value only adds estimated size of value itself
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        jsonMapper.writeValueAsString(ImmutableMap.of("x", "foo")),
        false
    );
    Assert.assertEquals(112, key.getEffectiveSizeBytes());
    Assert.assertEquals(1, indexer.getCardinality());

    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(jsonMapper.writeValueAsString(10L), false);
    Assert.assertEquals(94, key.getEffectiveSizeBytes());
    Assert.assertEquals(2, indexer.getCardinality());

    // adding same value only adds estimated size of value itself
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(jsonMapper.writeValueAsString(10L), false);
    Assert.assertEquals(16, key.getEffectiveSizeBytes());
    Assert.assertEquals(2, indexer.getCardinality());
    // new raw value, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(jsonMapper.writeValueAsString(11L), false);
    Assert.assertEquals(48, key.getEffectiveSizeBytes());
    Assert.assertEquals(3, indexer.getCardinality());

    // new raw value, new fields
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        jsonMapper.writeValueAsString(ImmutableList.of(1L, 2L, 10L)),
        false
    );
    Assert.assertEquals(276, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value, re-use fields and dictionary
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        jsonMapper.writeValueAsString(ImmutableList.of(1L, 2L, 10L)),
        false
    );
    Assert.assertEquals(56, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value, new fields
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        jsonMapper.writeValueAsString(ImmutableMap.of("x", ImmutableList.of(1L, 2L, 10L))),
        false
    );
    Assert.assertEquals(292, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());
    // new raw value
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        jsonMapper.writeValueAsString(ImmutableMap.of("x", ImmutableList.of(1L, 2L, 10L))),
        false
    );
    Assert.assertEquals(118, key.getEffectiveSizeBytes());
    Assert.assertEquals(5, indexer.getCardinality());

    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        jsonMapper.writeValueAsString(""),
        false
    );
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0, key.getEffectiveSizeBytes());
      Assert.assertEquals(6, indexer.getCardinality());
    } else {
      Assert.assertEquals(104, key.getEffectiveSizeBytes());
      Assert.assertEquals(6, indexer.getCardinality());
    }
  }

  @Test
  public void testJsonIndexerStringsParseException()
  {
    // same checks as testIndexer but json strings
    JsonColumnIndexer indexer = new JsonColumnIndexer();
    ObjectMapper jsonMapper = new DefaultObjectMapper();
    Assert.assertEquals(0, indexer.getCardinality());

    EncodedKeyComponent<StructuredData> key;
    // new raw value, new field, new dictionary entry
    key = indexer.processRowValsToUnsortedEncodedKeyComponent(
        "{'foo':1",
        false
    );
    // without parse exceptions is processed as a null
    Assert.assertEquals(46, key.getEffectiveSizeBytes());
    Assert.assertEquals(0, indexer.getCardinality());
    Assert.assertThrows(ParseException.class, () ->
        indexer.processRowValsToUnsortedEncodedKeyComponent(
            "{'foo':1",
            true
        )
    );
  }

  private void testIndexer(NestedDataColumnIndexer indexer)
  {
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

    key = indexer.processRowValsToUnsortedEncodedKeyComponent("", false);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(0, key.getEffectiveSizeBytes());
      Assert.assertEquals(6, indexer.getCardinality());
    } else {
      Assert.assertEquals(104, key.getEffectiveSizeBytes());
      Assert.assertEquals(6, indexer.getCardinality());
    }
  }
}
