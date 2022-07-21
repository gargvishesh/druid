/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.storage.s3;

import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import io.imply.druid.sql.async.result.RetriableS3OutputStream;
import io.imply.druid.sql.async.result.S3OutputConfig;
import io.imply.druid.storage.StorageConnector;
import org.apache.druid.data.input.impl.RetryingInputStream;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.storage.s3.S3Utils;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

public class S3StorageConnector implements StorageConnector
{
  private final S3OutputConfig config;
  private final ImplyServerSideEncryptingAmazonS3 s3Client;

  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();

  public S3StorageConnector(S3OutputConfig config, ImplyServerSideEncryptingAmazonS3 serverSideEncryptingAmazonS3)
  {
    this.config = config;
    this.s3Client = serverSideEncryptingAmazonS3;
  }

  @Override
  public boolean pathExists(String path)
  {
    return s3Client.doesObjectExist(config.getBucket(), objectPath(path));
  }

  @Override
  public InputStream read(String path) throws IOException
  {
    return new RetryingInputStream<GetObjectRequest>(
        new GetObjectRequest(config.getBucket(), objectPath(path)),
        new ObjectOpenFunction<GetObjectRequest>()
        {
          @Override
          public InputStream open(GetObjectRequest object)
          {
            return s3Client.getObject(object).getObjectContent();
          }

          @Override
          public InputStream open(GetObjectRequest object, long offset)
          {
            final GetObjectRequest offsetObjectRequest = new GetObjectRequest(
                object.getBucketName(),
                object.getKey()
            );
            offsetObjectRequest.setRange(offset);
            return open(offsetObjectRequest);
          }
        },
        S3Utils.S3RETRY,
        config.getMaxTriesOnTransientError()
    );
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    return new RetriableS3OutputStream(config, s3Client, objectPath(path));
  }

  @Override
  public void delete(String path)
  {
    s3Client.deleteObject(config.getBucket(), objectPath(path));
  }

  @Override
  public void deleteRecursively(String dirName)
  {
    ListObjectsV2Request listObjectsRequest = new ListObjectsV2Request()
        .withBucketName(config.getBucket())
        .withPrefix(objectPath(dirName));
    ListObjectsV2Result objectListing = s3Client.listObjectsV2(listObjectsRequest);

    while (objectListing.getObjectSummaries().size() > 0) {
      List<DeleteObjectsRequest.KeyVersion> deleteObjectsRequestKeys = objectListing.getObjectSummaries()
                                                                                    .stream()
                                                                                    .map(S3ObjectSummary::getKey)
                                                                                    .map(DeleteObjectsRequest.KeyVersion::new)
                                                                                    .collect(Collectors.toList());
      DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(config.getBucket()).withKeys(deleteObjectsRequestKeys);
      s3Client.deleteObjects(deleteObjectsRequest);

      // If the listing is truncated, all S3 objects have been deleted, otherwise, fetch more using the continuation token
      if (objectListing.isTruncated()) {
        listObjectsRequest.withContinuationToken(objectListing.getContinuationToken());
        objectListing = s3Client.listObjectsV2(listObjectsRequest);
      } else {
        break;
      }
    }
  }

  @Nonnull
  private String objectPath(String path)
  {
    return JOINER.join(config.getPrefix(), path);
  }

}
