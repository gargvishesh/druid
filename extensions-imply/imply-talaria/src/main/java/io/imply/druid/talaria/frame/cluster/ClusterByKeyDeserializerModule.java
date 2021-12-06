/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster;

import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.druid.segment.column.RowSignature;

/**
 * A module for deserialization of {@link ClusterByKey}.
 */
public class ClusterByKeyDeserializerModule extends SimpleModule
{
  public ClusterByKeyDeserializerModule(final RowSignature frameSignature, final ClusterBy clusterBy)
  {
    addDeserializer(
        ClusterByKey.class,
        ClusterByKeyDeserializer.fromClusterByAndRowSignature(clusterBy, frameSignature)
    );
  }
}
