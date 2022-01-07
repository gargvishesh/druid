/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

/**
 * Temporary combined task client to allow Alpha indexer-based code to
 * fit into the Talaria server framework.
 */
// TODO(paul): Remove this interface when the execution code works
// with the two client types directly.
public interface TalariaTaskClient extends LeaderClient, WorkerClient
{

}
