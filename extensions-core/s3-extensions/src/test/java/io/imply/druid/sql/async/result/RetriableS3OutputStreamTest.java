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

import com.amazonaws.AmazonClientException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import io.imply.druid.sql.async.InMemorySqlAsyncMetadataManager;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.storage.s3.ImplyServerSideEncryptingAmazonS3;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.s3.NoopServerSideEncryption;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class RetriableS3OutputStreamTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final SqlAsyncMetadataManager metadataManager = new InMemorySqlAsyncMetadataManager();
  private final TestAmazonS3 s3 = new TestAmazonS3(0);
  private final SqlAsyncQueryDetails queryDetails = SqlAsyncQueryDetails.createNew(
      "resultId",
      "identity",
      ResultFormat.OBJECT
  );

  private S3SqlAsyncResultManagerConfig config;
  private long maxResultsSize;
  private long chunkSize;

  @Before
  public void setup() throws IOException
  {
    final File tempDir = temporaryFolder.newFolder();
    chunkSize = 10L;
    config = new S3SqlAsyncResultManagerConfig()
    {
      @Override
      public File getTempDir()
      {
        return tempDir;
      }

      @Override
      public Long getChunkSize()
      {
        return chunkSize;
      }

      @Override
      public long getMaxResultsSize()
      {
        return maxResultsSize;
      }

      @Override
      public int getMaxTriesOnTransientError()
      {
        return 2;
      }
    };
  }

  @Test
  public void testTooSmallChunkSize() throws IOException
  {
    maxResultsSize = 100000000000L;
    chunkSize = 9000000L;

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "chunkSize[9000000] is too small for maxResultsSize[100000000000]. chunkSize should be at least [10000000]"
    );
    RetriableS3OutputStream.create(config, metadataManager, s3, queryDetails);
  }

  @Test
  public void testTooSmallChunkSizeMaxResultsSizeIsNotRetionalToMaxPartNum() throws IOException
  {
    maxResultsSize = 274877906944L;
    chunkSize = 27487790;

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "chunkSize[27487790] is too small for maxResultsSize[274877906944]. chunkSize should be at least [27487791]"
    );
    RetriableS3OutputStream.create(config, metadataManager, s3, queryDetails);
  }

  @Test
  public void testTooLargeChunkSize() throws IOException
  {
    maxResultsSize = 1024L * 1024 * 1024 * 1024;
    chunkSize = RetriableS3OutputStream.S3_MULTIPART_UPLOAD_MAX_PART_SIZE + 1;

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage(
        "chunkSize[5368709121] should be smaller than [5368709120]"
    );
    RetriableS3OutputStream.create(config, metadataManager, s3, queryDetails);
  }

  @Test
  public void testWriteAndHappy() throws IOException
  {
    maxResultsSize = 1000;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetriableS3OutputStream out = RetriableS3OutputStream.createWithoutChunkSizeValidation(
        config,
        metadataManager,
        s3,
        queryDetails
    )) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }
    // each chunk will be 8 bytes, so there should be 13 chunks.
    Assert.assertEquals(13, s3.partRequests.size());
    s3.assertCompleted();
  }

  @Test
  public void testWriteSizeLargerThanConfiguredMaxChunkSizeShouldSucceed() throws IOException
  {
    maxResultsSize = 1000;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES * 3);
    try (RetriableS3OutputStream out = RetriableS3OutputStream.createWithoutChunkSizeValidation(
        config,
        metadataManager,
        s3,
        queryDetails
    )) {
      bb.clear();
      bb.putInt(1);
      bb.putInt(2);
      bb.putInt(3);
      out.write(bb.array());
    }
    // each chunk will be 8 bytes, so there should be 13 chunks.
    Assert.assertEquals(1, s3.partRequests.size());
    s3.assertCompleted();
  }

  @Test
  public void testHitResultsSizeLimit() throws IOException
  {
    maxResultsSize = 50;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetriableS3OutputStream out = RetriableS3OutputStream.createWithoutChunkSizeValidation(
        config,
        metadataManager,
        s3,
        queryDetails
    )) {
      for (int i = 0; i < 14; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }

      Assert.assertThrows(
          "Exceeded max results size [50]",
          IOException.class,
          () -> {
            bb.clear();
            bb.putInt(14);
            out.write(bb.array());
          }
      );
    }

    s3.assertAborted();
  }

  @Test
  public void testSuccessToUploadAfterRetry() throws IOException
  {
    final TestAmazonS3 s3 = new TestAmazonS3(1);

    maxResultsSize = 1000;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetriableS3OutputStream out = RetriableS3OutputStream.createWithoutChunkSizeValidation(
        config,
        metadataManager,
        s3,
        queryDetails
    )) {
      for (int i = 0; i < 25; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }
    }
    // each chunk will be 8 bytes, so there should be 13 chunks.
    Assert.assertEquals(13, s3.partRequests.size());
    s3.assertCompleted();
  }

  @Test
  public void testFailToUploadAfterRetries() throws IOException
  {
    final TestAmazonS3 s3 = new TestAmazonS3(3);

    maxResultsSize = 1000;
    ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES);
    try (RetriableS3OutputStream out = RetriableS3OutputStream.createWithoutChunkSizeValidation(
        config,
        metadataManager,
        s3,
        queryDetails
    )) {
      for (int i = 0; i < 2; i++) {
        bb.clear();
        bb.putInt(i);
        out.write(bb.array());
      }

      expectedException.expect(RuntimeException.class);
      expectedException.expectCause(CoreMatchers.instanceOf(AmazonClientException.class));
      expectedException.expectMessage("Upload failure test. Remaining failures [1]");
      bb.clear();
      bb.putInt(3);
      out.write(bb.array());
    }

    s3.assertAborted();
  }

  private static class TestAmazonS3 extends ImplyServerSideEncryptingAmazonS3
  {
    private final List<UploadPartRequest> partRequests = new ArrayList<>();

    private int uploadFailuresLeft;
    private boolean aborted = false;
    @Nullable
    private CompleteMultipartUploadRequest completeRequest;

    private TestAmazonS3(int totalUploadFailures)
    {
      super(EasyMock.createMock(AmazonS3.class), new NoopServerSideEncryption());
      this.uploadFailuresLeft = totalUploadFailures;
    }

    @Override
    public InitiateMultipartUploadResult initiateMultipartUpload(InitiateMultipartUploadRequest request)
        throws SdkClientException
    {
      InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
      result.setUploadId("uploadId");
      return result;
    }

    @Override
    public UploadPartResult uploadPart(UploadPartRequest request) throws SdkClientException
    {
      if (uploadFailuresLeft > 0) {
        throw new AmazonClientException(
            new IOE("Upload failure test. Remaining failures [%s]", --uploadFailuresLeft)
        );
      }
      partRequests.add(request);
      UploadPartResult result = new UploadPartResult();
      result.setETag(StringUtils.format("%s", request.getPartNumber()));
      result.setPartNumber(request.getPartNumber());
      return result;
    }

    @Override
    public void abortMultipartUpload(AbortMultipartUploadRequest request) throws SdkClientException
    {
      aborted = true;
    }

    @Override
    public CompleteMultipartUploadResult completeMultipartUpload(CompleteMultipartUploadRequest request)
        throws SdkClientException
    {
      completeRequest = request;
      return new CompleteMultipartUploadResult();
    }

    private void assertCompleted()
    {
      Assert.assertNotNull(completeRequest);
      Assert.assertFalse(aborted);

      int prevPartId = 0;
      for (UploadPartRequest request : partRequests) {
        Assert.assertEquals(prevPartId + 1, request.getPartNumber());
        prevPartId++;
      }
      final List<PartETag> eTags = completeRequest.getPartETags();
      Assert.assertEquals(partRequests.size(), eTags.size());
      Assert.assertEquals(
          partRequests.stream().map(UploadPartRequest::getPartNumber).collect(Collectors.toList()),
          eTags.stream().map(PartETag::getPartNumber).collect(Collectors.toList())
      );
      Assert.assertEquals(
          partRequests.stream().map(UploadPartRequest::getPartNumber).collect(Collectors.toList()),
          eTags.stream().map(tag -> Integer.parseInt(tag.getETag())).collect(Collectors.toList())
      );
    }

    private void assertAborted()
    {
      Assert.assertTrue(aborted);
      Assert.assertNull(completeRequest);
    }
  }
}
