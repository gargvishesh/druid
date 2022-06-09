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

package io.imply.druid.sql.async.result;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import io.imply.druid.storage.s3.ImplyServerSideEncryptingAmazonS3;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.apache.druid.storage.s3.S3Utils;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class S3SqlAsyncResultManagerTest
{
  private static final AmazonS3 S3_CLIENT = EasyMock.createMock(AmazonS3Client.class);
  private static final ImplyServerSideEncryptingAmazonS3 AMAZON_S3 = new ImplyServerSideEncryptingAmazonS3(
      S3_CLIENT,
      new NoopServerSideEncryption()
  );

  private static final List<URI> EXPECTED_URIS = Arrays.asList(
      URI.create("s3://async-test/no-sync/query1"),
      URI.create("s3://async-test/no-sync/query2"),
      URI.create("s3://async-test/no-sync/query3")
  );

  private static final DateTime NOW = DateTimes.nowUtc();
  private static final byte[] CONTENT =
      StringUtils.toUtf8(StringUtils.format("%d,hello,world", NOW.getMillis()));

  @Test
  public void testAllAsyncResultIds() throws IOException
  {
    final S3SqlAsyncResultManager resultManager = new S3SqlAsyncResultManager(
        AMAZON_S3,
        new S3OutputConfig()
        {
          @Override
          public String getBucket()
          {
            return "async-test";
          }

          @Override
          public String getPrefix()
          {
            return "no-sync";
          }
        }
    );
    EasyMock.reset(S3_CLIENT);
    expectListObjects(URI.create("s3://async-test/no-sync"), EXPECTED_URIS, CONTENT);
    EasyMock.replay(S3_CLIENT);
    Assert.assertEquals(
        ImmutableList.of("query1", "query2", "query3"),
        resultManager.getAllAsyncResultIds()
    );
  }

  private void expectListObjects(URI prefix, List<URI> uris, byte[] content)
  {
    final ListObjectsV2Result result = new ListObjectsV2Result();
    result.setBucketName(prefix.getAuthority());
    result.setKeyCount(uris.size());
    for (URI uri : uris) {
      final String bucket = uri.getAuthority();
      final String key = S3Utils.extractS3Key(uri);
      final S3ObjectSummary objectSummary = new S3ObjectSummary();
      objectSummary.setBucketName(bucket);
      objectSummary.setKey(key);
      objectSummary.setSize(content.length);
      result.getObjectSummaries().add(objectSummary);
    }

    EasyMock.expect(
        S3_CLIENT.listObjectsV2(matchListObjectsRequest(prefix))
    ).andReturn(result).once();
  }

  private ListObjectsV2Request matchListObjectsRequest(final URI prefixUri)
  {
    // Use an IArgumentMatcher to verify that the request has the correct bucket and prefix.
    EasyMock.reportMatcher(
        new IArgumentMatcher()
        {
          @Override
          public boolean matches(Object argument)
          {
            if (!(argument instanceof ListObjectsV2Request)) {
              return false;
            }

            final ListObjectsV2Request request = (ListObjectsV2Request) argument;
            return prefixUri.getAuthority().equals(request.getBucketName())
                   && S3Utils.extractS3Key(prefixUri).equals(request.getPrefix());
          }

          @Override
          public void appendTo(StringBuffer buffer)
          {
            buffer.append("<request for prefix [").append(prefixUri).append("]>");
          }
        }
    );

    return null;
  }
}
