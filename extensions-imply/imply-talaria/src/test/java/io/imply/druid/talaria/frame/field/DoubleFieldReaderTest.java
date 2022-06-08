/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.field;

import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.quality.Strictness;

import java.util.Objects;

public class DoubleFieldReaderTest extends InitializedNullHandlingTest
{
  private static final long MEMORY_POSITION = 1;

  @Rule
  public MockitoRule mockitoRule = MockitoJUnit.rule().strictness(Strictness.STRICT_STUBS);

  @Mock
  public BaseDoubleColumnValueSelector writeSelector;

  private WritableMemory memory;
  private FieldWriter fieldWriter;

  @Before
  public void setUp()
  {
    memory = WritableMemory.allocate(1000);
    fieldWriter = new DoubleFieldWriter(writeSelector);
  }

  @After
  public void tearDown()
  {
    fieldWriter.close();
  }

  @Test
  public void test_makeColumnValueSelector_defaultOrNull()
  {
    writeToMemory(NullHandling.defaultDoubleValue());

    final ColumnValueSelector<?> readSelector =
        new DoubleFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertEquals(!NullHandling.replaceWithDefault(), readSelector.isNull());

    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(NullHandling.defaultDoubleValue(), readSelector.getDouble(), 0);
    }
  }

  @Test
  public void test_makeColumnValueSelector_aValue()
  {
    writeToMemory(5.1d);

    final ColumnValueSelector<?> readSelector =
        new DoubleFieldReader().makeColumnValueSelector(memory, new ConstantFieldPointer(MEMORY_POSITION));

    Assert.assertEquals(5.1d, readSelector.getObject());
  }

  @Test
  public void test_makeDimensionSelector_defaultOrNull()
  {
    writeToMemory(NullHandling.defaultDoubleValue());

    final DimensionSelector readSelector =
        new DoubleFieldReader().makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION), null);

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals(NullHandling.replaceWithDefault() ? "0.0" : null, readSelector.lookupName(0));

    // Informational method tests.
    Assert.assertFalse(readSelector.supportsLookupNameUtf8());
    Assert.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assert.assertEquals(String.class, readSelector.classOfObject());
    Assert.assertNull(readSelector.idLookup());

    // Value matcher tests.
    if (NullHandling.replaceWithDefault()) {
      Assert.assertTrue(readSelector.makeValueMatcher("0.0").matches());
      Assert.assertFalse(readSelector.makeValueMatcher((String) null).matches());
      Assert.assertTrue(readSelector.makeValueMatcher("0.0"::equals).matches());
      Assert.assertFalse(readSelector.makeValueMatcher(Objects::isNull).matches());
    } else {
      Assert.assertFalse(readSelector.makeValueMatcher("0.0").matches());
      Assert.assertTrue(readSelector.makeValueMatcher((String) null).matches());
      Assert.assertFalse(readSelector.makeValueMatcher("0.0"::equals).matches());
      Assert.assertTrue(readSelector.makeValueMatcher(Objects::isNull).matches());
    }
  }

  @Test
  public void test_makeDimensionSelector_aValue()
  {
    writeToMemory(5.1d);

    final DimensionSelector readSelector =
        new DoubleFieldReader().makeDimensionSelector(memory, new ConstantFieldPointer(MEMORY_POSITION), null);

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals("5.1", readSelector.lookupName(0));

    // Informational method tests.
    Assert.assertFalse(readSelector.supportsLookupNameUtf8());
    Assert.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assert.assertEquals(String.class, readSelector.classOfObject());
    Assert.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assert.assertTrue(readSelector.makeValueMatcher("5.1").matches());
    Assert.assertFalse(readSelector.makeValueMatcher("5").matches());
    Assert.assertTrue(readSelector.makeValueMatcher("5.1"::equals).matches());
    Assert.assertFalse(readSelector.makeValueMatcher("5"::equals).matches());
  }

  @Test
  public void test_makeDimensionSelector_aValue_extractionFn()
  {
    writeToMemory(20.5d);

    final DimensionSelector readSelector =
        new DoubleFieldReader().makeDimensionSelector(
            memory,
            new ConstantFieldPointer(MEMORY_POSITION),
            new SubstringDimExtractionFn(1, null)
        );

    // Data retrieval tests.
    final IndexedInts row = readSelector.getRow();
    Assert.assertEquals(1, row.size());
    Assert.assertEquals("0.5", readSelector.lookupName(0));

    // Informational method tests.
    Assert.assertFalse(readSelector.supportsLookupNameUtf8());
    Assert.assertFalse(readSelector.nameLookupPossibleInAdvance());
    Assert.assertEquals(DimensionDictionarySelector.CARDINALITY_UNKNOWN, readSelector.getValueCardinality());
    Assert.assertEquals(String.class, readSelector.classOfObject());
    Assert.assertNull(readSelector.idLookup());

    // Value matcher tests.
    Assert.assertTrue(readSelector.makeValueMatcher("0.5").matches());
    Assert.assertFalse(readSelector.makeValueMatcher("2").matches());
    Assert.assertTrue(readSelector.makeValueMatcher("0.5"::equals).matches());
    Assert.assertFalse(readSelector.makeValueMatcher("2"::equals).matches());
  }

  private void writeToMemory(final Double value)
  {
    Mockito.when(writeSelector.isNull()).thenReturn(value == null);

    if (value != null) {
      Mockito.when(writeSelector.getDouble()).thenReturn(value);
    }

    if (fieldWriter.writeTo(memory, MEMORY_POSITION, memory.getCapacity() - MEMORY_POSITION) < 0) {
      throw new ISE("Could not write");
    }
  }
}
