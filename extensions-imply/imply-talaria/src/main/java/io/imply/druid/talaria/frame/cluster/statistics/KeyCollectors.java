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

  public static KeyCollectorFactory<?> makeFactory(
      final ClusterBy clusterBy,
      final RowSignature signature,
      final boolean aggregate
  )
  {
    if (aggregate) {
      return DistinctKeyCollectorFactory.create(clusterBy, signature);
    } else {
      return QuantilesSketchKeyCollectorFactory.create(clusterBy, signature);
    }
  }
}
