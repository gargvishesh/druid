/*
 *
 *  * Copyright (c) Imply Data, Inc. All rights reserved.
 *  *
 *  * This software is the confidential and proprietary information
 *  * of Imply Data, Inc. You shall not disclose such Confidential
 *  * Information and shall use it only in accordance with the terms
 *  * of the license agreement you entered into with Imply.
 *
 *
 */

package io.imply.druid.inet.utils;

import inet.ipaddr.IPAddressSeqRange;
import inet.ipaddr.IPAddressString;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class IpAddressUtilsTest
{
  @Test
  public void testGetPossibleRangesFromIncompleteIpForBothIpv4AndIpv6() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("2");
    Assert.assertEquals(7, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("2.0.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("2.255.255.255").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("20.0.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("29.255.255.255").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("200.0.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("255.255.255.255").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("2::").getAddress().toIPv6().spanWithRange(new IPAddressString("2:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("20::").getAddress().toIPv6().spanWithRange(new IPAddressString("2f:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("200::").getAddress().toIPv6().spanWithRange(new IPAddressString("2ff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("2000::").getAddress().toIPv6().spanWithRange(new IPAddressString("2fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));

    ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("9");
    Assert.assertEquals(6, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("9.0.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("9.255.255.255").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("90.0.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("99.255.255.255").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("9::").getAddress().toIPv6().spanWithRange(new IPAddressString("9:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("90::").getAddress().toIPv6().spanWithRange(new IPAddressString("9f:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("900::").getAddress().toIPv6().spanWithRange(new IPAddressString("9ff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("9000::").getAddress().toIPv6().spanWithRange(new IPAddressString("9fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));

    ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("99");
    Assert.assertEquals(4, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("99.0.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("99.255.255.255").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("99::").getAddress().toIPv6().spanWithRange(new IPAddressString("99:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("990::").getAddress().toIPv6().spanWithRange(new IPAddressString("99f:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("9900::").getAddress().toIPv6().spanWithRange(new IPAddressString("99ff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForOnlyIpv6Letter() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("e");
    Assert.assertEquals(4, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("e::").getAddress().toIPv6().spanWithRange(new IPAddressString("e:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("e0::").getAddress().toIPv6().spanWithRange(new IPAddressString("ef:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("e00::").getAddress().toIPv6().spanWithRange(new IPAddressString("eff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("e000::").getAddress().toIPv6().spanWithRange(new IPAddressString("efff:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForCompleteIpv4CanExpand() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("192.168.1.1");
    Assert.assertEquals(3, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("192.168.1.1").getAddress().toIPv6().spanWithRange(new IPAddressString("192.168.1.1").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("192.168.1.10").getAddress().toIPv6().spanWithRange(new IPAddressString("192.168.1.19").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("192.168.1.100").getAddress().toIPv6().spanWithRange(new IPAddressString("192.168.1.199").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForCompleteIpv4CannotExpand() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("192.168.1.100");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("192.168.1.100").getAddress().toIPv6().spanWithRange(new IPAddressString("192.168.1.100").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForCompleteIpv6CanExpand() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("2001:4d98:bffb:ff01::1");
    Assert.assertEquals(4, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("2001:4d98:bffb:ff01::1").getAddress().toIPv6().spanWithRange(new IPAddressString("2001:4d98:bffb:ff01::1").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("2001:4d98:bffb:ff01::10").getAddress().toIPv6().spanWithRange(new IPAddressString("2001:4d98:bffb:ff01::1f").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("2001:4d98:bffb:ff01::100").getAddress().toIPv6().spanWithRange(new IPAddressString("2001:4d98:bffb:ff01::1ff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("2001:4d98:bffb:ff01::1000").getAddress().toIPv6().spanWithRange(new IPAddressString("2001:4d98:bffb:ff01::1fff").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForCompleteIpv6CannotExpand() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("2001:4d98:bffb:ff01::1ee2");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("2001:4d98:bffb:ff01::1ee2").getAddress().toIPv6().spanWithRange(new IPAddressString("2001:4d98:bffb:ff01::1ee2").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForCompleteIpv4WithIncompletePrefix() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("192.168.1.1/");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("192.168.1.1").getAddress().toIPv6().spanWithRange(new IPAddressString("192.168.1.1").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForCompleteIpv4WithCompletePrefix() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("192.168.1.1/16");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("192.168.1.1/16").getAddress().toIPv6().toPrefixBlock().toSequentialRange()));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForCompleteIpv6WithIncompletePrefix() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("::ffff:c0a8:1c7/");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("::ffff:c0a8:1c7").getAddress().toIPv6().spanWithRange(new IPAddressString("::ffff:c0a8:1c7").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForCompleteIpv6WithCompletePrefix() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("::ffff:c0a8:1c7/16");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("::ffff:c0a8:1c7/16").getAddress().toIPv6().toPrefixBlock().toSequentialRange()));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForIpv4() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("192.9");
    Assert.assertEquals(2, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("192.9.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("192.9.255.255").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("192.90.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("192.99.255.255").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForIpv4NotOverRange() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("192.90");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("192.90.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("192.90.255.255").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForIpv6() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("1234:ffff:c0");
    Assert.assertEquals(3, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("1234:ffff:c0::").getAddress().toIPv6().spanWithRange(new IPAddressString("1234:ffff:c0:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("1234:ffff:c00::").getAddress().toIPv6().spanWithRange(new IPAddressString("1234:ffff:c0f:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("1234:ffff:c000::").getAddress().toIPv6().spanWithRange(new IPAddressString("1234:ffff:c0ff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForIpv4EndingWithSeperator() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("192.168.");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("192.168.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("192.168.255.255").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForIpv6EndingWithSeperator() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("::ffff:c0a8:");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("::ffff:c0a8:0000").getAddress().toIPv6().spanWithRange(new IPAddressString("::ffff:c0a8:ffff").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForIpv6OnlyNumeric() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("2001");
    Assert.assertEquals(1, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("2001::").getAddress().toIPv6().spanWithRange(new IPAddressString("2001:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));

    ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("301");
    Assert.assertEquals(2, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("301::").getAddress().toIPv6().spanWithRange(new IPAddressString("301:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("3010::").getAddress().toIPv6().spanWithRange(new IPAddressString("301f:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
  }

  @Test
  public void testGetPossibleRangesFromIncompleteIpForIpv4AndIpv6OnlyNumeric() {
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("201");
    Assert.assertEquals(3, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("201::").getAddress().toIPv6().spanWithRange(new IPAddressString("201:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("2010::").getAddress().toIPv6().spanWithRange(new IPAddressString("201f:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("201.0.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("201.255.255.255").getAddress().toIPv6())));

    ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp("255");
    Assert.assertEquals(3, ranges.size());
    Assert.assertTrue(ranges.contains(new IPAddressString("255::").getAddress().toIPv6().spanWithRange(new IPAddressString("255:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("2550::").getAddress().toIPv6().spanWithRange(new IPAddressString("255f:ffff:ffff:ffff:ffff:ffff:ffff:ffff").getAddress().toIPv6())));
    Assert.assertTrue(ranges.contains(new IPAddressString("255.0.0.0").getAddress().toIPv6().spanWithRange(new IPAddressString("255.255.255.255").getAddress().toIPv6())));
  }
}
