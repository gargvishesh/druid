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

package io.imply.druid.inet.column;

import inet.ipaddr.IPAddressString;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.nio.ByteBuffer;

public class IpPrefixBlobTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(IpPrefixBlob.class).usingGetClass().verify();
  }

  @Test
  public void testFromStringNoPrefix()
  {
    String v6 = "1:2:3:0:0:6::";
    IPAddressString stringv6 = new IPAddressString(v6);
    byte[] addressAndPrefixByte = new byte[17];
    System.arraycopy(stringv6.getAddress().getBytes(), 0, addressAndPrefixByte, 0, stringv6.getAddress().getBytes().length);
    addressAndPrefixByte[addressAndPrefixByte.length - 1] = new Integer(128).byteValue();
    IpPrefixBlob blobv6 = IpPrefixBlob.ofString(v6);
    Assert.assertArrayEquals(addressAndPrefixByte, blobv6.getBytes());


    String v4 = "1.2.3.4";
    IPAddressString stringv4 = new IPAddressString(v4);
    System.arraycopy(stringv4.getAddress().toIPv6().getBytes(), 0, addressAndPrefixByte, 0, stringv4.getAddress().toIPv6().getBytes().length);
    addressAndPrefixByte[addressAndPrefixByte.length - 1] = new Integer(128).byteValue();
    IpPrefixBlob blobv4 = IpPrefixBlob.ofString(v4);
    Assert.assertArrayEquals(addressAndPrefixByte, blobv4.getBytes());
  }

  @Test
  public void testFromStrinInvalid()
  {
    String v6 = "1111123:2:3:0:0:6::";
    Assert.assertNull(IpPrefixBlob.ofString(v6));

    String v4 = "1.21231.3.4";
    Assert.assertNull(IpPrefixBlob.ofString(v4));
  }

  @Test
  public void testFromString()
  {
    String v6 = "1:2:3:0:0:6::/128";
    IPAddressString stringv6 = new IPAddressString(v6);
    byte[] addressAndPrefixByte = new byte[17];
    System.arraycopy(stringv6.getAddress().getBytes(), 0, addressAndPrefixByte, 0, stringv6.getAddress().getBytes().length);
    addressAndPrefixByte[addressAndPrefixByte.length - 1] = stringv6.getNetworkPrefixLength().byteValue();
    IpPrefixBlob blobv6 = IpPrefixBlob.ofString(v6);
    Assert.assertArrayEquals(addressAndPrefixByte, blobv6.getBytes());

    String v4 = "1.2.3.4/32";
    IPAddressString stringv4 = new IPAddressString(v4);
    System.arraycopy(stringv4.getAddress().toIPv6().getBytes(), 0, addressAndPrefixByte, 0, stringv4.getAddress().toIPv6().getBytes().length);
    addressAndPrefixByte[addressAndPrefixByte.length - 1] = stringv4.getAddress().toIPv6().getNetworkPrefixLength().byteValue();
    IpPrefixBlob blobv4 = IpPrefixBlob.ofString(v4);
    Assert.assertArrayEquals(addressAndPrefixByte, blobv4.getBytes());
  }

  @Test
  public void testFromBuffer()
  {
    String v6 = "1:2:3:0:0:6::/128";
    IpPrefixBlob blobv6 = IpPrefixBlob.ofString(v6);
    IpPrefixBlob blobv6FromBuffer = IpPrefixBlob.ofByteBuffer(ByteBuffer.wrap(blobv6.getBytes()));
    Assert.assertEquals(blobv6, blobv6FromBuffer);

    String v4 = "1.2.3.4/24";
    IpPrefixBlob blobv4 = IpPrefixBlob.ofString(v4);
    IpPrefixBlob blobv4FromBuffer = IpPrefixBlob.ofByteBuffer(ByteBuffer.wrap(blobv4.getBytes()));
    Assert.assertEquals(blobv4, blobv4FromBuffer);
  }

  @Test
  public void testToHost()
  {
    String v6 = "1:2:3:0:0:6::/36";
    IpPrefixBlob blobv6 = IpPrefixBlob.ofString(v6);
    IpAddressBlob actualBlob = blobv6.toHost();
    IpAddressBlob expectedBlob = IpAddressBlob.ofString("1:2:3:0:0:6::");
    Assert.assertEquals(expectedBlob, actualBlob);

    String v4 = "1.2.3.4/24";
    IpPrefixBlob blobv4 = IpPrefixBlob.ofString(v4);
    actualBlob = blobv4.toHost();
    expectedBlob = IpAddressBlob.ofString("1.2.3.4");
    Assert.assertEquals(expectedBlob, actualBlob);
  }

  @Test
  public void testParse()
  {
    String v4 = "1.2.3.4/32";
    IPAddressString stringv4 = new IPAddressString(v4);
    IpPrefixBlob blobv4 = IpPrefixBlob.parse(v4, true);
    byte[] addressAndPrefixByte = new byte[17];
    System.arraycopy(stringv4.getAddress().toIPv6().getBytes(), 0, addressAndPrefixByte, 0, stringv4.getAddress().toIPv6().getBytes().length);
    addressAndPrefixByte[addressAndPrefixByte.length - 1] = stringv4.getAddress().toIPv6().getNetworkPrefixLength().byteValue();
    Assert.assertArrayEquals(addressAndPrefixByte, blobv4.getBytes());
  }

  @Test
  public void testParseBadFormat()
  {
    expectedException.expect(ParseException.class);
    expectedException.expectMessage("Cannot parse [not an ip] as an IP prefix");
    IpPrefixBlob.parse("not an ip", true);
  }

  @Test
  public void testParseBadFormatNoReport()
  {
    Assert.assertNull(IpPrefixBlob.parse("not an ip", false));
  }
}
