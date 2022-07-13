/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.timeline.DataSegment;

/**
 * Interface for {@link org.apache.druid.rpc.ServiceClient}-backed communication with the Coordinator.
 */
public interface CoordinatorServiceClient
{
  ListenableFuture<DataSegment> fetchUsedSegment(String dataSource, String segmentId);

  CoordinatorServiceClient withRetryPolicy(ServiceRetryPolicy retryPolicy);
}
