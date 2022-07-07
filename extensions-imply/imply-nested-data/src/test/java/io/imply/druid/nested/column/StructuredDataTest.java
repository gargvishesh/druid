/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.nested.column;

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.Assert;
import org.junit.Test;

public class StructuredDataTest
{
  @Test
  public void testCompareTo()
  {
    StructuredData sd0 = new StructuredData(null);
    StructuredData sd1 = new StructuredData("hello");
    StructuredData sd2 = new StructuredData("world");
    StructuredData sd3 = new StructuredData(1L);
    StructuredData sd4 = new StructuredData(2L);
    StructuredData sd5 = new StructuredData(1.1);
    StructuredData sd6 = new StructuredData(3.3);
    StructuredData sd7 = new StructuredData(ImmutableMap.of("x", 1, "y", "hello"));
    StructuredData sd8 = new StructuredData(ImmutableMap.of("x", 1, "y", "hello"));
    StructuredData sd9 = new StructuredData(ImmutableMap.of("x", 12, "y", "world"));

    // equals
    Assert.assertEquals(0, sd0.compareTo(new StructuredData(null)));
    Assert.assertEquals(0, sd1.compareTo(new StructuredData("hello")));
    Assert.assertEquals(0, sd3.compareTo(new StructuredData(1L)));
    Assert.assertEquals(0, sd6.compareTo(new StructuredData(3.3)));
    Assert.assertEquals(0, sd7.compareTo(sd8));
    Assert.assertEquals(0, sd8.compareTo(sd7));

    // null comparison
    Assert.assertEquals(-1, sd0.compareTo(sd1));
    Assert.assertEquals(1, sd1.compareTo(sd0));

    // string comparison
    Assert.assertTrue(0 > sd1.compareTo(sd2));
    Assert.assertTrue(0 < sd2.compareTo(sd1));

    // long comparison
    Assert.assertEquals(-1, sd3.compareTo(sd4));
    Assert.assertEquals(1, sd4.compareTo(sd3));

    // double comparison
    Assert.assertEquals(-1, sd5.compareTo(sd6));
    Assert.assertEquals(1, sd6.compareTo(sd5));

    // number comparison
    Assert.assertEquals(-1, sd3.compareTo(sd5));
    Assert.assertEquals(1, sd5.compareTo(sd3));

    // object hash comparison
    Assert.assertEquals(1, sd7.compareTo(sd9));
    Assert.assertEquals(-1, sd9.compareTo(sd7));

    // test transitive
    Assert.assertEquals(-1, sd1.compareTo(sd3));
    Assert.assertEquals(1, sd3.compareTo(sd1));
    Assert.assertEquals(-1, sd2.compareTo(sd3));
    Assert.assertEquals(1, sd3.compareTo(sd2));

    Assert.assertEquals(-1, sd1.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd1));
    Assert.assertEquals(-1, sd2.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd2));

    Assert.assertEquals(-1, sd3.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd3));
    Assert.assertEquals(-1, sd4.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd4));
    Assert.assertEquals(-1, sd5.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd5));
    Assert.assertEquals(-1, sd6.compareTo(sd9));
    Assert.assertEquals(1, sd9.compareTo(sd6));


  }

  @Test
  public void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(StructuredData.class).withIgnoredFields("hashInitialized", "hashValue", "hash").usingGetClass().verify();
  }
}
