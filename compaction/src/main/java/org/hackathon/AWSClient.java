package org.hackathon;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;

import java.util.Set;
import java.util.stream.Collectors;

public class AWSClient
{
  final String bucket;
  final AmazonS3 s3Client;

  final Integer maxListing;

  public AWSClient(String bucket, Integer maxListing)
  {
    this.bucket = bucket;
//    ClientConfiguration clientConfiguration = new ClientConfiguration().
    s3Client = AmazonS3Client.builder().withRegion("us-east-1").build();
    this.maxListing = maxListing;
  }

  Set<String> getObjects(String prefix)
  {
    ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(prefix)
        .withMaxKeys(maxListing);

    ListObjectsV2Result results = s3Client.listObjectsV2(request);
    Set<String> objects = results.getObjectSummaries()
                                 .stream()
                                 .map(summary -> summary.getKey())
                                 .collect(Collectors.toSet());
    return objects;
  }

  void deleteObjects(Set<String> keys)
  {
    DeleteObjectsRequest request = new DeleteObjectsRequest(bucket);

    request.setKeys(keys.stream().map(key -> new DeleteObjectsRequest.KeyVersion(key)).collect(Collectors.toList()));

    DeleteObjectsResult results = s3Client.deleteObjects(request);
  }
}
