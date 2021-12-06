/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import org.apache.druid.java.util.common.StringUtils;

public class TooManyBucketsException extends RuntimeException
{
  private final int maxBuckets;

  public TooManyBucketsException(final int maxBuckets)
  {
    super(StringUtils.format("Too many buckets; maximum is [%s]", maxBuckets));
    this.maxBuckets = maxBuckets;
  }

  public int getMaxBuckets()
  {
    return maxBuckets;
  }
}
