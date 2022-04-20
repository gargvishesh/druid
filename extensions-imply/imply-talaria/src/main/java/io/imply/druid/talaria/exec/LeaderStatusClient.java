/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.google.common.base.Optional;
import org.apache.druid.indexer.TaskStatus;


/**
 * Generic Talaria interface that allows the workers to fetch the status of their leader/controller task
 */
public interface LeaderStatusClient
{
  /**
   * @return an {@code Optional.of()} with the current status of the leader task or
   * {@code Optional.absent()} if the task could not be found
   */
  Optional<TaskStatus> status();

  /**
   * Cleanup and close this client object
   */
  void close();
}
