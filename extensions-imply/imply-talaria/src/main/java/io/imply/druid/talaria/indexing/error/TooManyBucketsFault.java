/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName(TooManyBucketsFault.CODE)
public class TooManyBucketsFault extends BaseTalariaFault
{
  static final String CODE = "TooManyBuckets";

  private final int maxBuckets;

  @JsonCreator
  public TooManyBucketsFault(@JsonProperty("maxBuckets") final int maxBuckets)
  {
    // Currently, partition buckets are only used for segmentGranularity during ingestion queries. So it's fair
    // to assume that a TooManyBuckets error happened due to a too-fine segmentGranularity, even though we don't
    // technically have proof of that.
    super(
        CODE,
        "Too many partition buckets (max = %,d); try breaking your query up into smaller queries or "
        + "using a wider segmentGranularity",
        maxBuckets
    );
    this.maxBuckets = maxBuckets;
  }

  @JsonProperty
  public int getMaxBuckets()
  {
    return maxBuckets;
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
    if (!super.equals(o)) {
      return false;
    }
    TooManyBucketsFault that = (TooManyBucketsFault) o;
    return maxBuckets == that.maxBuckets;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), maxBuckets);
  }
}
