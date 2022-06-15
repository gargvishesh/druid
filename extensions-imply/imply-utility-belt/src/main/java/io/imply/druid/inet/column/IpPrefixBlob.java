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
    final Integer prefix = addressString.getNetworkPrefixLength();
    if (prefix == null) {
      return null;
    }
    // always store as ipv6 for now...
    byte[] addressBytes = address.toIPv6().getBytes();
    byte[] addressAndPrefixByte = new byte[17];
    System.arraycopy(addressBytes, 0, addressAndPrefixByte, 0, addressBytes.length);
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

  public String asCompressedString()
  {
    IPAddress addr = new IPv6Address(Arrays.copyOfRange(bytes, 0, bytes.length - 1));
    String addressString;
    if (addr.isIPv4Convertible()) {
      addressString = addr.toIPv4().toCompressedString();
    } else {
      addressString = addr.toCompressedString();
    }
    return addressString + "/" + prefixByteToString(bytes[bytes.length - 1]);
  }

  private String prefixByteToString(byte prefixByte) {
    return String.valueOf(Byte.toUnsignedInt(prefixByte));
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
