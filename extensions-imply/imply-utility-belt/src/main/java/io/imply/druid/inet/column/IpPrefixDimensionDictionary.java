package io.imply.druid.inet.column;

import org.apache.druid.segment.DimensionDictionary;

/**
 * DimensionDictionary for {@link IpPrefixBlob} dimension type.
 */
public class IpPrefixDimensionDictionary extends DimensionDictionary<IpPrefixBlob>
{
  private final boolean computeOnHeapSize;

  /**
   * Creates an IpPrefixDimensionDictionary.
   *
   * @param computeOnHeapSize true if on-heap memory estimation of the dictionary
   *                          size should be enabled, false otherwise
   */
  public IpPrefixDimensionDictionary(boolean computeOnHeapSize)
  {
    super(IpPrefixBlob.class);
    this.computeOnHeapSize = computeOnHeapSize;
  }

  @Override
  public long estimateSizeOfValue(IpPrefixBlob value)
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
