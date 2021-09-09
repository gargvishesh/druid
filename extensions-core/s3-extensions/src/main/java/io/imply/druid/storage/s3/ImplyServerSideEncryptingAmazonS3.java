/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.imply.druid.storage.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;
import org.apache.druid.storage.s3.ServerSideEncryption;

public class ImplyServerSideEncryptingAmazonS3 extends ServerSideEncryptingAmazonS3
{
  private final AmazonS3 amazonS3;
  private final ServerSideEncryption serverSideEncryption; // TODO: need to support it later

  public ImplyServerSideEncryptingAmazonS3(
      AmazonS3 amazonS3,
      ServerSideEncryption serverSideEncryption
  )
  {
    super(amazonS3, serverSideEncryption);
    this.amazonS3 = amazonS3;
    this.serverSideEncryption = serverSideEncryption;
  }

  public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
      throws SdkClientException
  {
    return amazonS3.initiateMultipartUpload(request);
  }

  public UploadPartResult uploadPart(UploadPartRequest request)
      throws SdkClientException
  {
    return amazonS3.uploadPart(request);
  }

  public void abortMultipartUpload(AbortMultipartUploadRequest request)
      throws SdkClientException
  {
    amazonS3.abortMultipartUpload(request);
  }

  public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
      throws SdkClientException
  {
    return amazonS3.completeMultipartUpload(request);
  }
}
