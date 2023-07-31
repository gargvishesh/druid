/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package org.apache.datasketches.hll;

public class ImplyHllSketchCompat
{
  public static HllSketch makeWithCoupons(int lgK, IndexValuePairs couponPairs)
  {
    HllSketch retVal = new HllSketch(lgK, TgtHllType.HLL_8);

    while (couponPairs.nextValid()) {
      int index = couponPairs.getIndex();
      int value = couponPairs.getValue();
      retVal.couponUpdate((value << 26) | (index & 0x03ffffff));
    }

    retVal.hllSketchImpl.putRebuildCurMinNumKxQFlag(true);
    retVal.hllSketchImpl.putOutOfOrder(true);
    return retVal;
  }

  public interface IndexValuePairs
  {
    boolean nextValid();

    int getIndex();

    int getValue();
  }
}
