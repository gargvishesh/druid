/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.jobs;

/**
 * todo: i guess this could be runnable, but idk if we need per run context like idk, number of active tasks on cluster
 *       or something to allow duties to decide to chill for example
 */
public interface JobProcessorDuty
{
  void run();
}
