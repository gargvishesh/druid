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
import io.imply.druid.talaria.frame.Frame;
import io.imply.druid.talaria.frame.file.FrameFile;
import org.apache.druid.java.util.common.IAE;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * TODO(gianm): Javadocs
 */
public class ReadableFileFrameChannel implements ReadableFrameChannel
{
  private final FrameFile frameFile;
  private final int endFrame;
  private int currentFrame;

  public ReadableFileFrameChannel(final FrameFile frameFile, final int startFrame, final int endFrame)
  {
    this.frameFile = frameFile;
    this.currentFrame = startFrame;
    this.endFrame = endFrame;

    if (startFrame < 0) {
      throw new IAE("startFrame[%,d] < 0", startFrame);
    }

    if (startFrame > endFrame) {
      throw new IAE("startFrame[%,d] > endFrame[%,d]", startFrame, endFrame);
    }

    if (endFrame > frameFile.numFrames()) {
      throw new IAE("endFrame[%,d] > numFrames[%,d]", endFrame, frameFile.numFrames());
    }
  }

  public ReadableFileFrameChannel(final FrameFile frameFile)
  {
    this(frameFile, 0, frameFile.numFrames());
  }

  @Override
  public boolean isFinished()
  {
    return currentFrame == endFrame;
  }

  @Override
  public boolean canRead()
  {
    return !isFinished();
  }

  @Override
  public Try<Frame> read()
  {
    if (isFinished()) {
      throw new NoSuchElementException();
    }

    return Try.value(frameFile.frame(currentFrame++));
  }

  @Override
  public ListenableFuture<?> readabilityFuture()
  {
    return Futures.immediateFuture(true);
  }

  @Override
  public void doneReading()
  {
    try {
      frameFile.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Returns a new reference to the {@link FrameFile} that this channel is reading from. Callers should close this
   * reference when done reading.
   */
  public FrameFile newFrameFileReference()
  {
    return frameFile.newReference();
  }
}
