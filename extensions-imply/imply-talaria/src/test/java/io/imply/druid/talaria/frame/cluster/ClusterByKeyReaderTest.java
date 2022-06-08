/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class ClusterByKeyReaderTest extends InitializedNullHandlingTest
{
  private final RowSignature signature =
      RowSignature.builder()
                  .add("long", ColumnType.LONG)
                  .add("longDefault", ColumnType.LONG)
                  .add("float", ColumnType.FLOAT)
                  .add("floatDefault", ColumnType.FLOAT)
                  .add("string", ColumnType.STRING)
                  .add("stringNull", ColumnType.STRING)
                  .add("multiValueString", ColumnType.STRING)
                  .add("double", ColumnType.DOUBLE)
                  .add("doubleDefault", ColumnType.DOUBLE)
                  .add("stringArray", ColumnType.STRING_ARRAY)
                  .build();

  private final List<Object> objects = Arrays.asList(
      5L,
      NullHandling.defaultLongValue(),
      6f,
      NullHandling.defaultFloatValue(),
      "foo",
      null,
      Arrays.asList("bar", "qux"),
      7d,
      NullHandling.defaultDoubleValue(),
      Arrays.asList("abc", "xyz")
  );

  private final ClusterByKey key = ClusterByTestUtils.createKey(signature, objects.toArray());

  private final ClusterByKeyReader keyReader = ClusterByKeyReader.create(signature);

  @Test
  public void test_read_all()
  {
    Assert.assertEquals(objects, keyReader.read(key));
  }

  @Test
  public void test_read_oneField()
  {
    for (int i = 0; i < signature.size(); i++) {
      Assert.assertEquals(
          "read: " + signature.getColumnName(i),
          objects.get(i),
          keyReader.read(key, i)
      );
    }
  }

  @Test
  public void test_hasMultipleValues()
  {
    for (int i = 0; i < signature.size(); i++) {
      Assert.assertEquals(
          "hasMultipleValues: " + signature.getColumnName(i),
          objects.get(i) instanceof List,
          keyReader.hasMultipleValues(key, i)
      );
    }
  }

  @Test
  public void test_trim_zero()
  {
    Assert.assertEquals(ClusterByKey.empty(), keyReader.trim(key, 0));
  }

  @Test
  public void test_trim_one()
  {
    Assert.assertEquals(
        ClusterByTestUtils.createKey(
            RowSignature.builder().add(signature.getColumnName(0), signature.getColumnType(0).get()).build(),
            objects.get(0)
        ),
        keyReader.trim(key, 1)
    );
  }

  @Test
  public void test_trim_oneLessThanFullLength()
  {
    final int numFields = signature.size() - 1;
    RowSignature.Builder trimmedSignature = RowSignature.builder();
    IntStream.range(0, numFields)
             .forEach(i -> trimmedSignature.add(signature.getColumnName(i), signature.getColumnType(i).get()));

    Assert.assertEquals(
        ClusterByTestUtils.createKey(trimmedSignature.build(), objects.subList(0, numFields).toArray()),
        keyReader.trim(key, numFields)
    );
  }

  @Test
  public void test_trim_fullLength()
  {
    Assert.assertEquals(key, keyReader.trim(key, signature.size()));
  }

  @Test
  public void test_trim_beyondFullLength()
  {
    final IllegalArgumentException e = Assert.assertThrows(
        IllegalArgumentException.class,
        () -> keyReader.trim(key, signature.size() + 1)
    );

    MatcherAssert.assertThat(e, ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString("Cannot trim")));
  }
}
