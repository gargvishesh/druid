/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import io.imply.druid.talaria.frame.cluster.ClusterBy;
import org.apache.druid.segment.column.RowSignature;

public class KeyCollectors
{
  private KeyCollectors()
  {
    // No instantiation.
  }

  /**
   * Used by {@link ClusterByStatisticsCollectorImpl#create} and anything else that seeks to have the same behavior.
   */
  public static KeyCollectorFactory<?, ?> makeStandardFactory(
      final ClusterBy clusterBy,
      final RowSignature signature,
      final boolean aggregate
  )
  {
    final KeyCollectorFactory<?, ?> baseFactory;

    if (aggregate) {
      baseFactory = DistinctKeyCollectorFactory.create(clusterBy, signature);
    } else {
      baseFactory = QuantilesSketchKeyCollectorFactory.create(clusterBy, signature);
    }

    // Wrap in DelegateOrMinKeyCollectorFactory, so we are guaranteed to be able to downsample to a single key. This
    // is important because it allows us to better handle large numbers of small buckets.
    return new DelegateOrMinKeyCollectorFactory<>(clusterBy.keyComparator(signature), baseFactory);
  }
}
