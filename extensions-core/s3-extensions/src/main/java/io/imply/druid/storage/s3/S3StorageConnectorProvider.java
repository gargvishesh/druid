/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.storage.s3;


import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.imply.druid.sql.async.result.S3OutputConfig;
import io.imply.druid.storage.StorageConnector;
import io.imply.druid.storage.StorageConnectorProvider;
import org.apache.druid.storage.s3.S3StorageDruidModule;

@JsonTypeName(S3StorageDruidModule.SCHEME)
public class S3StorageConnectorProvider extends S3OutputConfig implements StorageConnectorProvider
{
  @JacksonInject
  ImplyServerSideEncryptingAmazonS3 s3;

  @Override
  public StorageConnector get()
  {
    return new S3StorageConnector(this, s3);
  }
}
