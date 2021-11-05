/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import inet.ipaddr.IPAddressString;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IpAddressBlobTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(IpAddressBlob.class).usingGetClass().verify();
  }

  @Test
  public void testFromString()
  {
    String v6 = "1:2:3:0:0:6::";
    IPAddressString stringv6 = new IPAddressString(v6);
    IpAddressBlob blobv6 = IpAddressBlob.ofString(v6);

    Assert.assertArrayEquals(stringv6.getAddress().getBytes(), blobv6.getBytes());

    String v4 = "1.2.3.4";
    IPAddressString stringv4 = new IPAddressString(v4);
    IpAddressBlob blobv4 = IpAddressBlob.ofString(v4);
    Assert.assertArrayEquals(stringv4.getAddress().toIPv6().getBytes(), blobv4.getBytes());
  }

  @Test
  public void testFromBuffer()
  {
    String v6 = "1:2:3:0:0:6::";
    IPAddressString stringv6 = new IPAddressString(v6);
    IpAddressBlob blobv6 = IpAddressBlob.ofString(v6);

    Assert.assertArrayEquals(stringv6.getAddress().getBytes(), blobv6.getBytes());

    String v4 = "1.2.3.4";
    IPAddressString stringv4 = new IPAddressString(v4);
    IpAddressBlob blobv4 = IpAddressBlob.ofString(v4);
    Assert.assertArrayEquals(stringv4.getAddress().toIPv6().getBytes(), blobv4.getBytes());
  }

  @Test
  public void testParse()
  {
    String v4 = "1.2.3.4";
    IPAddressString stringv4 = new IPAddressString(v4);
    IpAddressBlob blobv4 = IpAddressBlob.parse(v4, true);
    Assert.assertArrayEquals(stringv4.getAddress().toIPv6().getBytes(), blobv4.getBytes());
  }

  @Test
  public void testParseBadFormat()
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Cannot parse [not an ip] as an IP address");
    IpAddressBlob.parse("not an ip", true);
  }

  @Test
  public void testParseBadFormatNoReport()
  {
    Assert.assertNull(IpAddressBlob.parse("not an ip", false));
  }
}