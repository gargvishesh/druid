/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import io.imply.druid.talaria.frame.channel.ReadableFrameChannel;
import io.imply.druid.talaria.kernel.StageId;

import java.io.IOException;

public interface InputChannelFactory
{
  ReadableFrameChannel openChannel(StageId stageId, int workerNumber, int partitionNumber) throws IOException;
}
