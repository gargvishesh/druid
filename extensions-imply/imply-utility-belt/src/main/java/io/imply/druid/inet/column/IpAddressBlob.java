/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv6.IPv6Address;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class IpAddressBlob implements Comparable<IpAddressBlob>
{
  public static final Comparator<IpAddressBlob> COMPARATOR = Comparators.naturalNullsFirst();

  public static final Strategy STRATEGY = new Strategy();

  @Nullable
  public static IpAddressBlob parse(@Nullable Object input, boolean reportParseExceptions)
  {
    if (input == null) {
      return null;
    }
    final IpAddressBlob blob;
    if (input instanceof IpAddressBlob) {
      return (IpAddressBlob) input;
    }

    if (input instanceof String) {
      blob = IpAddressBlob.ofString((String) input);
    } else {
      throw new IAE("Cannot handle [%s]", input.getClass());
    }
    // blob should not be null if we get to here, a null is a parse exception
    if (blob == null && reportParseExceptions) {
      throw new ParseException(input.toString(), "Cannot parse [%s] as an IP address", input);
    }
    return blob;
  }


  @Nullable
  public static IpAddressBlob ofString(final String value)
  {
    final IPAddressString addressString = new IPAddressString(value);
    if (!addressString.isValid()) {
      return null;
    }
    final IPAddress address = addressString.getAddress();
    if (address == null) {
      return null;
    }
    // always store as ipv6 for now...
    return new IpAddressBlob(address.toIPv6().getBytes());
  }

  @Nullable
  public static IpAddressBlob ofByteBuffer(final ByteBuffer blob)
  {
    if (blob != null) {
      byte[] bytes = new byte[16];
      final int oldPosition = blob.position();
      blob.get(bytes, 0, 16);
      blob.position(oldPosition);
      return new IpAddressBlob(bytes);
    }
    return null;
  }

  private final byte[] bytes;

  public IpAddressBlob(byte[] bytes)
  {
    this.bytes = bytes;
  }

  @Override
  public int compareTo(IpAddressBlob o)
  {
    return ByteBuffer.wrap(getBytes()).compareTo(ByteBuffer.wrap(o.getBytes()));
  }

  public byte[] getBytes()
  {
    return bytes;
  }

  public String stringify(boolean compact, boolean forceV6)
  {
    IPAddress addr = new IPv6Address(bytes);
    if (!forceV6 && addr.isIPv4Convertible()) {
      if (compact) {
        return addr.toIPv4().toCompressedString();
      }
      return addr.toIPv4().toNormalizedString();
    }
    if (compact) {
      return addr.toCompressedString();
    }
    return addr.toFullString();
  }

  public IpPrefixBlob toPrefix(int length)
  {
    IPAddress addr = new IPv6Address(bytes);
    IPv6Address iPv6Address;
    if (addr.isIPv4Convertible()) {
      iPv6Address = addr.toIPv4().toPrefixBlock(length).toIPv6();
    } else {
      iPv6Address = addr.toIPv6().toPrefixBlock(length);
    }
    return IpPrefixBlob.ofIpv6Address(iPv6Address);
  }

  public boolean matches(String toMatch)
  {
    if (toMatch == null) {
      return false;
    }
    IPAddress addr = new IPv6Address(bytes);
    IPAddressString stringAddr = new IPAddressString(toMatch);
    if (!stringAddr.isValid()) {
      throw new IAE("Cannot match [%s] with invalid address [%s]", addr.toCompressedString(), toMatch);
    }
    IPAddress matchAddr = stringAddr.getAddress().toIPv6();
    return matchAddr.toPrefixBlock().contains(addr);
  }

  public String asCompressedString()
  {
    IPAddress addr = new IPv6Address(bytes);
    if (addr.isIPv4Convertible()) {
      return addr.toIPv4().toCompressedString();
    }
    return addr.toCompressedString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IpAddressBlob that = (IpAddressBlob) o;
    return Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(bytes);
  }

  @Override
  public String toString()
  {
    return asCompressedString();
  }

  public static class Strategy implements ObjectStrategy<IpAddressBlob>
  {
    @Override
    public Class<? extends IpAddressBlob> getClazz()
    {
      return IpAddressBlob.class;
    }

    @Nullable
    @Override
    public IpAddressBlob fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      return IpAddressBlob.ofByteBuffer(buffer);
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable IpAddressBlob val)
    {
      if (val == null) {
        return null;
      }
      return val.getBytes();
    }

    @Override
    public int compare(IpAddressBlob o1, IpAddressBlob o2)
    {
      return COMPARATOR.compare(o1, o2);
    }
  }
}
