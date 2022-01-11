/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.imply.druid.talaria.frame.cluster.ClusterBy;
import io.imply.druid.talaria.frame.cluster.ClusterByKeyDeserializerModule;
import io.imply.druid.talaria.frame.cluster.statistics.KeyCollectorFactory;
import io.imply.druid.talaria.frame.cluster.statistics.KeyCollectorSnapshotDeserializerModule;
import io.imply.druid.talaria.frame.cluster.statistics.KeyCollectors;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;

public class TalariaTasks
{
  /**
   * Returns a decorated copy of an ObjectMapper, enabled for the classes
   * {@link io.imply.druid.talaria.frame.cluster.ClusterByKey} and
   * {@link io.imply.druid.talaria.frame.cluster.statistics.KeyCollector}.
   */
  static ObjectMapper decorateObjectMapperForClusterByKey(
      final ObjectMapper mapper,
      final RowSignature frameSignature,
      final ClusterBy clusterBy,
      final boolean aggregate
  )
  {
    final KeyCollectorFactory<?> keyCollectorFactory = KeyCollectors.makeFactory(clusterBy, frameSignature, aggregate);

    final ObjectMapper mapperCopy = mapper.copy();
    mapperCopy.registerModule(new ClusterByKeyDeserializerModule(frameSignature, clusterBy));
    mapperCopy.registerModule(new KeyCollectorSnapshotDeserializerModule(keyCollectorFactory));
    return mapperCopy;
  }

  static String getHostFromSelfNode(@Nullable final DruidNode selfNode)
  {
    return selfNode != null ? selfNode.getHostAndPortToUse() : null;
  }
}
