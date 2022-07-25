/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.talaria.frame.file.FrameFileWriter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;

/**
 * On disk writeable channel that is backed by a {@link FrameFileWriter} and
 * produces {@link io.imply.druid.talaria.frame.file.FrameFile} when written to
 */
public class WritableStreamFrameChannel implements WritableFrameChannel
{
  private static final Logger log = new Logger(WritableStreamFrameChannel.class);

  private final FrameFileWriter writer;
  private boolean errorEncountered = false;

  public WritableStreamFrameChannel(final FrameFileWriter writer)
  {
    this.writer = writer;
  }

  @Override
  public void write(Try<FrameWithPartition> frameTry) throws IOException
  {
    if (frameTry.isValue()) {
      if (errorEncountered) {
        throw new ISE("Cannot write frame after writing error");
      }

      final FrameWithPartition frame = frameTry.getOrThrow();
      writer.writeFrame(frame.frame(), frame.partition());
    } else {
      // Can't write an error to a stream. Log and chomp it. Readers of the stream will be able to detect that
      // it has been truncated, so they'll know that there was an error of some kind.
      log.warn(frameTry.error(), "Error encountered; not writing it to stream");
      writer.abort();
      errorEncountered = true;
    }
  }

  @Override
  public void doneWriting() throws IOException
  {
    if (errorEncountered) {
      log.warn("Closing a writer on which error was encountered");
    }
    writer.close();
  }

  /**
   * This assumes that the local disk has infinite space to accomadate for all the data, and therefore resolves to true
   * all the time. This might not be the case if disk space is full.
   * This method should account for the disk space while resolving the future or spill to an external storage if it
   * wants to return true unconditionally
   */
  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    return Futures.immediateFuture(true);
  }
}
