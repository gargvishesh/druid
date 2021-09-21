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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;
import com.google.common.io.CountingOutputStream;
import io.imply.druid.sql.async.SqlAsyncUtil;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import io.imply.druid.storage.s3.ImplyServerSideEncryptingAmazonS3;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.s3.S3Utils;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * A retriable output stream for s3. How it works is,
 * <p>
 * 1) When new data is written, it first creates a chunk in local disk.
 * 2) New data is written to the local chunk until it is full.
 * 3) When the chunk is full, it uploads the chunk to s3 using the multipart upload API.
 * Since this happens synchronously, {@link #write(byte[], int, int)} can be blocked until the upload is done.
 * The upload can be retries when it fails with transient errors.
 * 4) Once the upload succeeds, it creates a new chunk and continue.
 * 5) When the stream is closed, it uploads the last chunk and finalize the multipart upload.
 * {@link #close()} can be blocked until upload is done.
 * <p>
 * For compression format support, this output stream supports compression formats if they are <i>concatenatable</i>,
 * such as ZIP or GZIP.
 * <p>
 * This class is not thread-safe.
 */
class RetriableS3OutputStream extends OutputStream
{
  public static final long S3_MULTIPART_UPLOAD_MIN_PART_SIZE = 5L * 1024 * 1024;
  public static final long S3_MULTIPART_UPLOAD_MAX_PART_SIZE = 5L * 1024 * 1024 * 1024L;

  private static final Logger LOG = new Logger(RetriableS3OutputStream.class);
  private static final Joiner JOINER = Joiner.on("/").skipNulls();
  private static final int S3_MULTIPART_UPLOAD_MAX_NUM_PARTS = 10_000;

  private final S3SqlAsyncResultManagerConfig config;
  private final SqlAsyncMetadataManager metadataManager;
  private final ImplyServerSideEncryptingAmazonS3 s3;
  private final String s3Key;
  private final String uploadId;
  private final File chunkStorePath;
  private final long chunkSize;

  private final List<PartETag> pushResults = new ArrayList<>();
  private final byte[] singularBuffer = new byte[1];

  // metric
  private final Stopwatch pushStopwatch;

  @MonotonicNonNull
  private Chunk currentChunk;
  private int nextChunkId = 1; // multipart upload requires partNumber to be in the range between 1 and 10000
  /**
   * Total size of all chunks. This size is updated whenever the chunk is ready for push,
   * not when {@link #write(byte[], int, int)} is called. This is because
   * it will be hard to know the increase of chunk size in write() when the chunk is compressed.
   */
  private long resultsSize;

  /**
   * A flag indicating whether there was an upload error.
   * This flag is tested in {@link #close()} to determine whether it needs to upload the current chunk or not.
   */
  private boolean error;
  private boolean closed;

  static RetriableS3OutputStream create(
      S3SqlAsyncResultManagerConfig config,
      SqlAsyncMetadataManager metadataManager,
      ImplyServerSideEncryptingAmazonS3 s3,
      SqlAsyncQueryDetails queryDetails
  ) throws IOException
  {
    final RetriableS3OutputStream stream = new RetriableS3OutputStream(config, metadataManager, s3, queryDetails);
    validateChunkSize(config.getMaxResultsSize(), stream.chunkSize);
    return stream;
  }

  @VisibleForTesting
  static RetriableS3OutputStream createWithoutChunkSizeValidation(
      S3SqlAsyncResultManagerConfig config,
      SqlAsyncMetadataManager metadataManager,
      ImplyServerSideEncryptingAmazonS3 s3,
      SqlAsyncQueryDetails queryDetails
  ) throws IOException
  {
    return new RetriableS3OutputStream(config, metadataManager, s3, queryDetails);
  }

  private RetriableS3OutputStream(
      S3SqlAsyncResultManagerConfig config,
      SqlAsyncMetadataManager metadataManager,
      ImplyServerSideEncryptingAmazonS3 s3,
      SqlAsyncQueryDetails queryDetails
  ) throws IOException
  {
    this.config = config;
    this.metadataManager = metadataManager;
    this.s3 = s3;
    this.s3Key = getS3KeyForQuery(config.getPrefix(), queryDetails.getAsyncResultId());

    final InitiateMultipartUploadResult result = s3.initiateMultipartUpload(
        new InitiateMultipartUploadRequest(config.getBucket(), s3Key)
    );
    this.uploadId = result.getUploadId();
    this.chunkStorePath = new File(config.getTempDir(), SqlAsyncUtil.safeId(queryDetails.getAsyncResultId()));
    FileUtils.forceMkdir(this.chunkStorePath);
    this.chunkSize = config.getChunkSize() == null ? computeChunkSize(config) : config.getChunkSize();
    this.pushStopwatch = Stopwatch.createUnstarted();
    this.pushStopwatch.reset();
  }

  private static long computeChunkSize(S3SqlAsyncResultManagerConfig config)
  {
    return computeMinChunkSize(config.getMaxResultsSize());
  }

  private static void validateChunkSize(long maxResultsSize, long chunkSize)
  {
    if (computeMinChunkSize(maxResultsSize) > chunkSize) {
      throw new IAE(
          "chunkSize[%s] is too small for maxResultsSize[%s]. chunkSize should be at least [%s]",
          chunkSize,
          maxResultsSize,
          computeMinChunkSize(maxResultsSize)
      );
    }
    if (S3_MULTIPART_UPLOAD_MAX_PART_SIZE < chunkSize) {
      throw new IAE(
          "chunkSize[%s] should be smaller than [%s]",
          chunkSize,
          S3_MULTIPART_UPLOAD_MAX_PART_SIZE
      );
    }
  }

  private static long computeMinChunkSize(long maxResultsSize)
  {
    return Math.max(
        (long) Math.ceil(maxResultsSize / (double) S3_MULTIPART_UPLOAD_MAX_NUM_PARTS),
        S3_MULTIPART_UPLOAD_MIN_PART_SIZE
    );
  }

  static String getS3KeyForQuery(String prefix, String asyncResultId)
  {
    return JOINER.join(
        prefix,
        SqlAsyncUtil.safeId(asyncResultId)
    );
  }

  @Override
  public void write(int b) throws IOException
  {
    singularBuffer[0] = (byte) b;
    write(singularBuffer, 0, 1);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException
  {
    if (b == null) {
      error = true;
      throw new NullPointerException();
    } else if ((off < 0) || (off > b.length) || (len < 0) ||
               ((off + len) > b.length) || ((off + len) < 0)) {
      error = true;
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }

    try {
      if (currentChunk == null || currentChunk.length() >= chunkSize) {
        pushCurrentChunk();
        currentChunk = new Chunk(nextChunkId, new File(chunkStorePath, String.valueOf(nextChunkId++)));
      }

      currentChunk.outputStream.write(b, off, len);
    }
    catch (RuntimeException | IOException e) {
      error = true;
      throw e;
    }
  }

  private void pushCurrentChunk() throws IOException
  {
    if (currentChunk != null) {
      currentChunk.close();
      final Chunk chunk = currentChunk;
      try {
        resultsSize += chunk.length();
        if (metadataManager.totalCompleteQueryResultsSize() + resultsSize > config.getMaxTotalResultsSize()) {
          throw new IOE("Exceeded max result store capacity [%s]", config.getMaxTotalResultsSize());
        }
        if (resultsSize > config.getMaxResultsSize()) {
          throw new IOE("Exceeded max results size [%s]", config.getMaxResultsSize());
        }

        pushStopwatch.start();
        pushResults.add(push(chunk));
        pushStopwatch.stop();
      }
      finally {
        if (!chunk.delete()) {
          LOG.warn("Failed to delete chunk [%s]", chunk.getAbsolutePath());
        }
      }
    }
  }

  private PartETag push(Chunk chunk) throws IOException
  {
    try {
      return RetryUtils.retry(
          () -> uploadPartIfPossible(uploadId, config.getBucket(), s3Key, chunk),
          S3Utils.S3RETRY,
          config.getMaxTriesOnTransientError()
      );
    }
    catch (AmazonServiceException e) {
      throw new IOException(e);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private PartETag uploadPartIfPossible(
      String uploadId,
      String bucket,
      String key,
      Chunk chunk
  )
  {
    final ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(resultsSize);
    final UploadPartRequest uploadPartRequest = new UploadPartRequest()
        .withUploadId(uploadId)
        .withBucketName(bucket)
        .withKey(key)
        .withFile(chunk.file)
        .withPartNumber(chunk.id)
        .withPartSize(chunk.length());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Pushing chunk [%s] to bucket[%s] and key[%s].", chunk, bucket, key);
    }
    UploadPartResult uploadResult = s3.uploadPart(uploadPartRequest);
    return uploadResult.getPartETag();
  }

  @Override
  public void close() throws IOException
  {
    if (closed) {
      return;
    }
    closed = true;
    Closer closer = Closer.create();

    closer.register(() -> {
      // This should be emitted as a metric
      LOG.info("Total push time: [%s] ms", pushStopwatch.elapsed(TimeUnit.MILLISECONDS));
    });

    // Closeables are closed in LIFO order
    closer.register(() -> FileUtils.forceDelete(chunkStorePath));

    closer.register(() -> {
      try {
        if (resultsSize > 0 && isAllPushSucceeded()) {
          RetryUtils.retry(
              () -> s3.completeMultipartUpload(
                  new CompleteMultipartUploadRequest(config.getBucket(), s3Key, uploadId, pushResults)
              ),
              S3Utils.S3RETRY,
              config.getMaxTriesOnTransientError()
          );
        } else {
          RetryUtils.retry(
              () -> {
                s3.abortMultipartUpload(new AbortMultipartUploadRequest(config.getBucket(), s3Key, uploadId));
                return null;
              },
              S3Utils.S3RETRY,
              config.getMaxTriesOnTransientError()
          );
        }
      }
      catch (Exception e) {
        throw new IOException(e);
      }
    });

    try (Closer ignored = closer) {
      if (!error) {
        pushCurrentChunk();
      }
    }
  }

  private boolean isAllPushSucceeded()
  {
    return !error && !pushResults.isEmpty() && nextChunkId - 1 == pushResults.size();
  }

  private static class Chunk implements Closeable
  {
    private final int id;
    private final File file;
    private final CountingOutputStream outputStream;
    private boolean closed;

    private Chunk(int id, File file) throws FileNotFoundException
    {
      this.id = id;
      this.file = file;
      this.outputStream = new CountingOutputStream(new FastBufferedOutputStream(new FileOutputStream(file)));
    }

    private long length()
    {
      return outputStream.getCount();
    }

    private boolean delete()
    {
      return file.delete();
    }

    private String getAbsolutePath()
    {
      return file.getAbsolutePath();
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
      Chunk chunk = (Chunk) o;
      return id == chunk.id;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(id);
    }

    @Override
    public void close() throws IOException
    {
      if (closed) {
        return;
      }
      closed = true;
      outputStream.close();
    }

    @Override
    public String toString()
    {
      return "Chunk{" +
             "id=" + id +
             ", file=" + file.getAbsolutePath() +
             ", size=" + length() +
             '}';
    }
  }
}
