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
import org.apache.druid.java.util.common.IAE;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;
import java.util.Map;

public class PathFinderTest
{

  private static final Map<String, Object> NESTER = ImmutableMap.of(
      "x", ImmutableList.of("a", "b", "c"),
      "y", ImmutableMap.of("a", "hello", "b", "world"),
      "z", "foo",
      "[sneaky]", "bar",
      "[also_sneaky]", ImmutableList.of(ImmutableMap.of("a", "x"), ImmutableMap.of("b", "y", "c", "z"))
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testParseJqPath()
  {
    List<PathFinder.PathPartFinder> pathParts;

    pathParts = PathFinder.parseJqPath(".");
    Assert.assertEquals(0, pathParts.size());

    // { "z" : "hello" }
    pathParts = PathFinder.parseJqPath(".z");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("z", pathParts.get(0).getPartName());
    Assert.assertEquals(".\"z\"", PathFinder.toNormalizedJqPath(pathParts));

    // { "z" : "hello" }
    pathParts = PathFinder.parseJqPath(".\"z\"");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("z", pathParts.get(0).getPartName());
    Assert.assertEquals(".\"z\"", PathFinder.toNormalizedJqPath(pathParts));

    // { "z" : "hello" }
    pathParts = PathFinder.parseJqPath(".[\"z\"]");
    Assert.assertEquals(1, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("z", pathParts.get(0).getPartName());
    Assert.assertEquals(".\"z\"", PathFinder.toNormalizedJqPath(pathParts));

    // { "x" : ["a", "b"]}
    pathParts = PathFinder.parseJqPath(".x[1]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.ArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertEquals(".\"x\"[1]", PathFinder.toNormalizedJqPath(pathParts));

    // { "x" : ["a", "b"]}
    pathParts = PathFinder.parseJqPath(".\"x\"[1]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.ArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertEquals(".\"x\"[1]", PathFinder.toNormalizedJqPath(pathParts));

    // { "x" : ["a", "b"]}
    pathParts = PathFinder.parseJqPath(".[\"x\"][1]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.ArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertEquals(".\"x\"[1]", PathFinder.toNormalizedJqPath(pathParts));

    // { "x" : { "1" : "hello" }}
    pathParts = PathFinder.parseJqPath(".[\"x\"][\"1\"]");
    Assert.assertEquals(2, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.MapField);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertEquals(".\"x\".\"1\"", PathFinder.toNormalizedJqPath(pathParts));


    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = PathFinder.parseJqPath(".x[1].foo.bar");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.ArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertTrue(pathParts.get(2) instanceof PathFinder.MapField);
    Assert.assertEquals("foo", pathParts.get(2).getPartName());
    Assert.assertTrue(pathParts.get(3) instanceof PathFinder.MapField);
    Assert.assertEquals("bar", pathParts.get(3).getPartName());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", PathFinder.toNormalizedJqPath(pathParts));

    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = PathFinder.parseJqPath(".x[1].\"foo\".bar");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.ArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertTrue(pathParts.get(2) instanceof PathFinder.MapField);
    Assert.assertEquals("foo", pathParts.get(2).getPartName());
    Assert.assertTrue(pathParts.get(3) instanceof PathFinder.MapField);
    Assert.assertEquals("bar", pathParts.get(3).getPartName());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", PathFinder.toNormalizedJqPath(pathParts));

    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = PathFinder.parseJqPath(".[\"x\"][1].\"foo\"[\"bar\"]");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.ArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertTrue(pathParts.get(2) instanceof PathFinder.MapField);
    Assert.assertEquals("foo", pathParts.get(2).getPartName());
    Assert.assertTrue(pathParts.get(3) instanceof PathFinder.MapField);
    Assert.assertEquals("bar", pathParts.get(3).getPartName());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", PathFinder.toNormalizedJqPath(pathParts));

    // make sure we chomp question marks
    // { "x" : [ { "foo" : { "bar" : "hello" }}, { "foo" : { "bar" : "world" }}]}
    pathParts = PathFinder.parseJqPath(".[\"x\"]?[1]?.foo?.\"bar\"?");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.ArrayElement);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertTrue(pathParts.get(2) instanceof PathFinder.MapField);
    Assert.assertEquals("foo", pathParts.get(2).getPartName());
    Assert.assertTrue(pathParts.get(3) instanceof PathFinder.MapField);
    Assert.assertEquals("bar", pathParts.get(3).getPartName());
    Assert.assertEquals(".\"x\"[1].\"foo\".\"bar\"", PathFinder.toNormalizedJqPath(pathParts));

    // { "x" : { "1" : { "foo" : { "bar" : "hello" }}}}
    pathParts = PathFinder.parseJqPath(".\"x\"[\"1\"].\"foo\".\"bar\"");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.MapField);
    Assert.assertEquals("1", pathParts.get(1).getPartName());
    Assert.assertTrue(pathParts.get(2) instanceof PathFinder.MapField);
    Assert.assertEquals("foo", pathParts.get(2).getPartName());
    Assert.assertTrue(pathParts.get(3) instanceof PathFinder.MapField);
    Assert.assertEquals("bar", pathParts.get(3).getPartName());
    Assert.assertEquals(".\"x\".\"1\".\"foo\".\"bar\"", PathFinder.toNormalizedJqPath(pathParts));

    // stress out the parser
    // { "x.y.z]?[\\\"]][]\" : { "13234.12[]][23" : { "f?o.o" : { ".b?.a.r.": "hello" }}}}
    pathParts = PathFinder.parseJqPath(".[\"x.y.z]?[\\\"]][]\"]?[\"13234.12[]][23\"].\"f?o.o\"?[\".b?.a.r.\"]");
    Assert.assertEquals(4, pathParts.size());
    Assert.assertTrue(pathParts.get(0) instanceof PathFinder.MapField);
    Assert.assertEquals("x.y.z]?[\\\"]][]", pathParts.get(0).getPartName());
    Assert.assertTrue(pathParts.get(1) instanceof PathFinder.MapField);
    Assert.assertEquals("13234.12[]][23", pathParts.get(1).getPartName());
    Assert.assertTrue(pathParts.get(2) instanceof PathFinder.MapField);
    Assert.assertEquals("f?o.o", pathParts.get(2).getPartName());
    Assert.assertTrue(pathParts.get(3) instanceof PathFinder.MapField);
    Assert.assertEquals(".b?.a.r.", pathParts.get(3).getPartName());
    Assert.assertEquals(".\"x.y.z]?[\\\"]][]\".\"13234.12[]][23\".\"f?o.o\".\".b?.a.r.\"", PathFinder.toNormalizedJqPath(pathParts));
  }

  @Test
  public void testBadFormatMustStartWithDot()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, 'x.y' is not a valid 'jq' path: must start with '.'");
    PathFinder.parseJqPath("x.y");
  }

  @Test
  public void testBadFormatNoDot()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage(".\"x\"\"y\"' is not a valid 'jq' path: path parts must be separated with '.'");
    PathFinder.parseJqPath(".\"x\"\"y\"");
  }

  @Test
  public void testBadFormatWithDot2()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '..\"x\"' is not a valid 'jq' path: path parts separated by '.' must not be empty");
    PathFinder.parseJqPath("..\"x\"");
  }

  @Test
  public void testBadFormatWithDot3()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x.[1]' is not a valid 'jq' path: invalid position 3 for '[', must not follow '.' or must be contained with '\"'");
    PathFinder.parseJqPath(".x.[1]");
  }

  @Test
  public void testBadFormatWithDot4()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[1].[2]' is not a valid 'jq' path: invalid position 6 for '[', must not follow '.' or must be contained with '\"'");
    PathFinder.parseJqPath(".x[1].[2]");
  }

  @Test
  public void testBadFormatNotANumber()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[.1]' is not a valid 'jq' path: expected number for array specifier got .1 instead. Use \"\" if this value was meant to be a field name");
    PathFinder.parseJqPath(".x[.1]");
  }

  @Test
  public void testBadFormatUnclosedArray()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[1' is not a valid 'jq' path: unterminated '['");
    PathFinder.parseJqPath(".x[1");
  }

  @Test
  public void testBadFormatUnclosedArray2()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[\"1\"' is not a valid 'jq' path: unterminated '['");
    PathFinder.parseJqPath(".x[\"1\"");
  }

  @Test
  public void testBadFormatUnclosedQuote()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x.\"1' is not a valid 'jq' path: unterminated '\"'");
    PathFinder.parseJqPath(".x.\"1");
  }

  @Test
  public void testBadFormatUnclosedQuote2()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Bad format, '.x[\"1]' is not a valid 'jq' path: unterminated '\"'");
    PathFinder.parseJqPath(".x[\"1]");
  }



  @Test
  public void testPathSplitter()
  {
    List<PathFinder.PathPartFinder> pathParts;

    pathParts = PathFinder.parseJqPath(".");
    Assert.assertEquals(NESTER, PathFinder.find(NESTER, pathParts));
    Assert.assertNull(PathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = PathFinder.parseJqPath(".z");
    Assert.assertEquals("foo", PathFinder.find(NESTER, pathParts));
    Assert.assertEquals("foo", PathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = PathFinder.parseJqPath(".x");
    Assert.assertEquals(NESTER.get("x"), PathFinder.find(NESTER, pathParts));
    Assert.assertNull(PathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = PathFinder.parseJqPath(".x[1]");
    Assert.assertEquals("b", PathFinder.find(NESTER, pathParts));
    Assert.assertEquals("b", PathFinder.findStringLiteral(NESTER, pathParts));

    // nonexistent
    pathParts = PathFinder.parseJqPath(".x[1].y.z");
    Assert.assertNull(PathFinder.find(NESTER, pathParts));
    Assert.assertNull(PathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = PathFinder.parseJqPath(".y.a");
    Assert.assertEquals("hello", PathFinder.find(NESTER, pathParts));
    Assert.assertEquals("hello", PathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = PathFinder.parseJqPath(".y[1]");
    Assert.assertNull(PathFinder.find(NESTER, pathParts));
    Assert.assertNull(PathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = PathFinder.parseJqPath(".\"[sneaky]\"");
    Assert.assertEquals("bar", PathFinder.find(NESTER, pathParts));
    Assert.assertEquals("bar", PathFinder.findStringLiteral(NESTER, pathParts));

    pathParts = PathFinder.parseJqPath(".\"[also_sneaky]\"[1].c");
    Assert.assertEquals("z", PathFinder.find(NESTER, pathParts));
    Assert.assertEquals("z", PathFinder.findStringLiteral(NESTER, pathParts));
  }
}
