/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.files.s3;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class S3FileStoreConfig
{
  @JsonProperty("bucket")
  private String bucket;

  public String getBucket()
  {
    return bucket;
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
    S3FileStoreConfig that = (S3FileStoreConfig) o;
    return Objects.equals(bucket, that.bucket);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(bucket);
  }
}
