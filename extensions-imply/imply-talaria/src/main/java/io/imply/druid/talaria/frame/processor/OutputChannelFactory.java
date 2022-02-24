/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import java.io.IOException;

/**
 * Factory for generating channel pairs for output data from processors.
 */
public interface OutputChannelFactory
{
  /**
   * Create a channel pair tagged with a particular partition number.
   */
  OutputChannel openChannel(int partitionNumber) throws IOException;

  /**
   * Create a non-writable, always-empty channel pair tagged with a particular partition number.
   *
   * Calling {@link OutputChannel#getWritableChannel()} on this nil channel pair will result in an error. Calling
   * {@link OutputChannel#getReadableChannel()} will return an empty channel.
   */
  OutputChannel openNilChannel(int partitionNumber);
}
