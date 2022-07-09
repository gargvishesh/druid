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
import inet.ipaddr.IPAddressSeqRange;
import inet.ipaddr.IPAddressString;
import inet.ipaddr.ipv6.IPv6Address;
import io.imply.druid.inet.utils.IpAddressUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class IpPrefixBlob implements Comparable<IpPrefixBlob>
{
  public static final Comparator<IpPrefixBlob> COMPARATOR = Comparators.naturalNullsFirst();

  public static final Strategy STRATEGY = new Strategy();

  @Nullable
  public static IpPrefixBlob parse(@Nullable Object input, boolean reportParseExceptions)
  {
    if (input == null) {
      return null;
    }
    final IpPrefixBlob blob;
    if (input instanceof IpPrefixBlob) {
      return (IpPrefixBlob) input;
    }

    if (input instanceof String) {
      blob = IpPrefixBlob.ofString((String) input);
    } else {
      throw new IAE("Cannot handle [%s]", input.getClass());
    }
    // blob should not be null if we get to here, a null is a parse exception
    if (blob == null && reportParseExceptions) {
      throw new ParseException(input.toString(), "Cannot parse [%s] as an IP prefix", input);
    }
    return blob;
  }

  @Nullable
  public static IpPrefixBlob ofString(final String value)
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
    return ofIpv6Address(address.toIPv6());
  }

  public static IpPrefixBlob ofIpv6Address(final IPv6Address iPv6Address) {
    Integer prefix = iPv6Address.getNetworkPrefixLength();
    if (prefix == null) {
      prefix = 128;
    }
    byte[] addressAndPrefixByte = new byte[17];
    System.arraycopy(iPv6Address.getBytes(), 0, addressAndPrefixByte, 0, iPv6Address.getBytes().length);
    addressAndPrefixByte[addressAndPrefixByte.length - 1] = prefix.byteValue();
    return new IpPrefixBlob(addressAndPrefixByte);
  }

  @Nullable
  public static IpPrefixBlob ofByteBuffer(final ByteBuffer blob)
  {
    if (blob != null) {
      byte[] bytes = new byte[17];
      final int oldPosition = blob.position();
      blob.get(bytes, 0, 17);
      blob.position(oldPosition);
      return new IpPrefixBlob(bytes);
    }
    return null;
  }

  private final byte[] bytes;

  public IpPrefixBlob(byte[] bytes)
  {
    this.bytes = bytes;
  }

  @Override
  public int compareTo(IpPrefixBlob o)
  {
    return ByteBuffer.wrap(getBytes()).compareTo(ByteBuffer.wrap(o.getBytes()));
  }

  public byte[] getBytes()
  {
    return bytes;
  }

  public String stringify(boolean compact, boolean forceV6)
  {
    return addressByteToString(compact, forceV6);
  }

  public IpAddressBlob toHost()
  {
    IPAddress addr = new IPv6Address(bytes, 0, bytes.length - 1, prefixByteToLong(bytes[bytes.length - 1]));
    return new IpAddressBlob(addr.toIPv6().getBytes());
  }

  public boolean matches(String toMatch)
  {
    if (toMatch == null) {
      return false;
    }
    IPAddress addr = new IPv6Address(bytes, 0, bytes.length - 1, prefixByteToLong(bytes[bytes.length - 1]));
    IPAddressString stringAddr = new IPAddressString(toMatch);
    if (!stringAddr.isValid()) {
      throw new IAE("Cannot match [%s] with invalid address [%s]", addr.toCompressedString(), toMatch);
    }
    IPAddress matchAddr = stringAddr.getAddress().toIPv6();
    return addr.toPrefixBlock().contains(matchAddr);
  }

  public boolean searches(String toMatch)
  {
    if (toMatch == null) {
      return false;
    }
    List<IPAddressSeqRange> ranges = IpAddressUtils.getPossibleRangesFromIncompleteIp(toMatch);
    IPAddress addr = new IPv6Address(bytes, 0, bytes.length - 1, prefixByteToLong(bytes[bytes.length - 1]));
    for (IPAddressSeqRange range : ranges) {
      if (range.intersect(addr.toPrefixBlock().toSequentialRange()) != null) {
        return true;
      }
    }
    return false;
  }

  public String asCompressedString()
  {
    return addressByteToString(true, false);
  }

  private String addressByteToString(boolean compact, boolean forceV6) {
    IPAddress addr = new IPv6Address(bytes, 0, bytes.length - 1, prefixByteToLong(bytes[bytes.length - 1]));
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

  private int prefixByteToLong(byte prefixByte) {
    return Byte.toUnsignedInt(prefixByte);
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
    IpPrefixBlob that = (IpPrefixBlob) o;
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

  public static class Strategy implements ObjectStrategy<IpPrefixBlob>
  {
    @Override
    public Class<? extends IpPrefixBlob> getClazz()
    {
      return IpPrefixBlob.class;
    }

    @Nullable
    @Override
    public IpPrefixBlob fromByteBuffer(ByteBuffer buffer, int numBytes)
    {
      return IpPrefixBlob.ofByteBuffer(buffer);
    }

    @Nullable
    @Override
    public byte[] toBytes(@Nullable IpPrefixBlob val)
    {
      if (val == null) {
        return null;
      }
      return val.getBytes();
    }

    @Override
    public int compare(IpPrefixBlob o1, IpPrefixBlob o2)
    {
      return COMPARATOR.compare(o1, o2);
    }
  }
}
