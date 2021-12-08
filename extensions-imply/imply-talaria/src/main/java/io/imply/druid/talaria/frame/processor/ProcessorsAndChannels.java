/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import org.apache.druid.java.util.common.guava.Sequence;

public class ProcessorsAndChannels<ProcessorClass extends FrameProcessor<T>, T>
{
  private final Sequence<ProcessorClass> workers;
  private final OutputChannels outputChannels;

  public ProcessorsAndChannels(
      final Sequence<ProcessorClass> workers,
      final OutputChannels outputChannels
  )
  {
    this.workers = workers;
    this.outputChannels = outputChannels;
  }

  public Sequence<ProcessorClass> processors()
  {
    return workers;
  }

  public OutputChannels getOutputChannels()
  {
    return outputChannels;
  }
}
