/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.frame.read.FrameReader;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

public class QueryWorkerInput
{
  @Nullable
  private final SegmentWithInterval segment;

  @Nullable
  private final ReadableFrameChannel inputChannel;

  @Nullable
  private final FrameReader inputFrameReader;

  private QueryWorkerInput(
      @Nullable SegmentWithInterval segment,
      @Nullable ReadableFrameChannel inputChannel,
      @Nullable FrameReader inputFrameReader
  )
  {
    this.segment = segment;
    this.inputChannel = inputChannel;
    this.inputFrameReader = inputFrameReader;

    if ((segment == null) == (inputChannel == null)) {
      throw new ISE("Provide either 'segment' or 'inputChannel'");
    }
  }

  public static QueryWorkerInput forSegment(final SegmentWithInterval segment)
  {
    return new QueryWorkerInput(segment, null, null);
  }

  public static QueryWorkerInput forInputChannel(final ReadableFrameChannel inputChannel, final FrameReader frameReader)
  {
    return new QueryWorkerInput(null, inputChannel, frameReader);
  }

  public boolean hasSegment()
  {
    return segment != null;
  }

  public boolean hasInputChannel()
  {
    return inputChannel != null;
  }

  public SegmentWithInterval getSegment()
  {
    return Preconditions.checkNotNull(segment, "segment");
  }

  public ReadableFrameChannel getInputChannel()
  {
    return Preconditions.checkNotNull(inputChannel, "inputChannel");
  }

  public FrameReader getInputFrameReader()
  {
    return Preconditions.checkNotNull(inputFrameReader, "inputFrameReader");
  }
}
