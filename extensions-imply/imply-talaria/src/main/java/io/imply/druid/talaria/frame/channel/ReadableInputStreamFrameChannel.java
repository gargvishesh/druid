/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.talaria.frame.Frame;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.io.InputStream;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class ReadableInputStreamFrameChannel implements ReadableFrameChannel
{

  private final InputStream inputStream;

  private final ReadableByteChunksFrameChannel delegate;

  private final Object lock = new Object();

  @GuardedBy("lock")
  private final byte[] buffer = new byte[8 * 1024];

  @GuardedBy("lock")
  private long totalInputStreamBytesRead = 0;

  @GuardedBy("lock")
  private boolean inputStreamFinished = false;

  @GuardedBy("lock")
  private boolean inputStreamError = false;


  private volatile boolean readingStarted = false;

  private volatile boolean keepReading = true;


  private ExecutorService executorService;


  public ReadableInputStreamFrameChannel(InputStream inputStream, String id, ExecutorService executorService)
  {
    this.inputStream = inputStream;
    delegate = ReadableByteChunksFrameChannel.create(id);
    this.executorService = executorService;
  }

  /**
   * Method needs to be called for reading of input streams into ByteChunksFrameChannel
   */

  public void startReading()
  {
    // submit the reading task to the executor service only once
    if (readingStarted) {
      return;
    } else {
      synchronized (lock) {
        if (readingStarted) {
          return;
        }
        readingStarted = true;
      }
      executorService.submit(() -> {
        while (true) {
          if (!keepReading) {
            try {
              //TODO: change to exponential backoff.
              Thread.sleep(100);
              synchronized (lock) {
                if (inputStreamFinished || inputStreamError || delegate.isErrorOrFinished()) {
                  return;
                }
              }
            }
            catch (InterruptedException e) {
              // close inputstream anyway if the thread interrups
              IOUtils.closeQuietly(inputStream);
              throw new ISE(e, Thread.currentThread().getName() + "interrupted");
            }

          } else {
            synchronized (lock) {

              // if done reading method is called we should not read input stream further
              if (inputStreamFinished) {
                delegate.doneWriting();
                break;
              }
              try {

                int bytesRead = inputStream.read(buffer);
                if (bytesRead == -1) {
                  inputStreamFinished = true;
                  delegate.doneWriting();
                  break;
                } else {
                  Optional<ListenableFuture<?>> futureOptional = delegate.addChunk(Arrays.copyOfRange(
                      buffer,
                      0,
                      bytesRead
                  ));
                  totalInputStreamBytesRead += bytesRead;
                  if (futureOptional.isPresent()) {
                    // backpressure handling
                    keepReading = false;
                    futureOptional.get().addListener(() -> keepReading = true, Execs.directExecutor());
                  } else {
                    keepReading = true;
                    // continue adding data to delegate
                    // give up lock so that other threads have a change to do some work
                  }
                }
              }
              catch (Exception e) {
                //handle exception
                long currentStreamOffset = totalInputStreamBytesRead;
                delegate.setError(new ISE(e,
                                          "Found error while reading input stream at %d", currentStreamOffset
                ));
                inputStreamError = true;
                // close the stream in case done reading is not called.
                IOUtils.closeQuietly(inputStream);
                break;
              }
            }
          }
        }
      });
    }
  }

  @Override
  public boolean isFinished()
  {
    synchronized (lock) {
      return delegate.isFinished();
    }
  }

  @Override
  public boolean canRead()
  {
    synchronized (lock) {
      if (!readingStarted) {
        throw new ISE("Please call startReading method before calling canRead()");
      }
      return delegate.canRead();
    }
  }

  @Override
  public Try<Frame> read()
  {
    synchronized (lock) {
      if (!readingStarted) {
        throw new ISE("Please call startReading method before calling read");
      }
      return delegate.read();
    }
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    synchronized (lock) {
      if (!readingStarted) {
        throw new ISE("Please call startReading method before calling readabilityFuture()");
      }
      return delegate.readabilityFuture();
    }
  }


  @Override
  public void doneReading()
  {
    synchronized (lock) {
      inputStreamFinished = true;
      delegate.doneReading();
      IOUtils.closeQuietly(inputStream);
    }
  }
}
