/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.channel;

import io.imply.druid.talaria.frame.file.FrameFile;
import org.junit.Test;

import java.util.NoSuchElementException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReadableFileFrameChannelTest
{

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidStartingFrame()
  {
    new ReadableFileFrameChannel(mock(FrameFile.class), -1, -1);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidEndLessThanStartFrame()
  {
    new ReadableFileFrameChannel(mock(FrameFile.class), 5, 3);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInvalidEndFrame()
  {
    FrameFile frameFile = mock(FrameFile.class);
    when(frameFile.numFrames()).thenReturn(4);
    new ReadableFileFrameChannel(frameFile, 1, 5);
  }

  @Test(expected = NoSuchElementException.class)
  public void readFinishedFrameFileThrowsException()
  {
    FrameFile frameFile = mock(FrameFile.class);
    when(frameFile.numFrames()).thenReturn(5);
    ReadableFileFrameChannel readableFileFrameChannel = new ReadableFileFrameChannel(frameFile, 3, 3);
    readableFileFrameChannel.read();
  }
}
