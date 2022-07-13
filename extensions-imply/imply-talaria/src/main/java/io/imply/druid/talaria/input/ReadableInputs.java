/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.input;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.read.FrameReader;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * Iterable sequence of {@link ReadableInput}.
 */
public class ReadableInputs implements Iterable<ReadableInput>
{
  private final Iterable<ReadableInput> iterable;

  @Nullable
  private final FrameReader frameReader;

  private ReadableInputs(Iterable<ReadableInput> iterable, @Nullable FrameReader frameReader)
  {
    this.iterable = Preconditions.checkNotNull(iterable, "iterable");
    this.frameReader = frameReader;
  }

  public static ReadableInputs channels(final Iterable<ReadableInput> iterable, FrameReader frameReader)
  {
    return new ReadableInputs(iterable, Preconditions.checkNotNull(frameReader, "frameReader"));
  }

  public static ReadableInputs segments(final Iterable<ReadableInput> iterable)
  {
    return new ReadableInputs(iterable, null);
  }

  @Override
  public Iterator<ReadableInput> iterator()
  {
    return iterable.iterator();
  }

  public FrameReader frameReader()
  {
    if (frameReader == null) {
      throw new ISE("No frame reader; check hasChannels() first");
    }

    return frameReader;
  }

  public boolean hasSegments()
  {
    return frameReader == null;
  }

  public boolean hasChannels()
  {
    return frameReader != null;
  }
}
