/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

public interface KeyCollectorFactory<CollectorType extends KeyCollector<CollectorType>>
{
  CollectorType newKeyCollector();

  Class<? extends KeyCollectorSnapshot> snapshotClass();

  KeyCollectorSnapshot toSnapshot(CollectorType collector);

  CollectorType fromSnapshot(KeyCollectorSnapshot snapshot);
}
