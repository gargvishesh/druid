/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.inet.column;

import org.apache.druid.segment.DimensionDictionary;

/**
 * DimensionDictionary for {@link IpAddressBlob} dimension type.
 */
public class IpAddressDimensionDictionary extends DimensionDictionary<IpAddressBlob>
{
  private final boolean computeOnHeapSize;

  /**
   * Creates an IpAddressDimensionDictionary.
   *
   * @param computeOnHeapSize true if on-heap memory estimation of the dictionary
   *                          size should be enabled, false otherwise
   */
  public IpAddressDimensionDictionary(boolean computeOnHeapSize)
  {
    super(IpAddressBlob.class);
    this.computeOnHeapSize = computeOnHeapSize;
  }

  @Override
  public long estimateSizeOfValue(IpAddressBlob value)
  {
    if (value == null) {
      return 0;
    }

    byte[] bytes = value.getBytes();

    // Size of byte array + 1 reference to byte array
    return (bytes == null ? 0 : bytes.length) + Long.BYTES;
  }

  @Override
  public boolean computeOnHeapSize()
  {
    return computeOnHeapSize;
  }
}
