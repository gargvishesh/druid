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
 * TODO(gianm): Javadocs
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
    // TODO(gianm): Do something special if there was an error?
    writer.close();
  }

  @Override
  public ListenableFuture<?> writabilityFuture()
  {
    // TODO(gianm): Should either return "not ready" when disk is full, or should spill to S3
    return Futures.immediateFuture(true);
  }
}
