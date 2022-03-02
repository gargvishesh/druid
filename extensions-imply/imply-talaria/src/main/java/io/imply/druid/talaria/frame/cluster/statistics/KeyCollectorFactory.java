/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.databind.JsonDeserializer;

public interface KeyCollectorFactory<TCollector extends KeyCollector<TCollector>, TSnapshot extends KeyCollectorSnapshot>
{
  TCollector newKeyCollector();

  JsonDeserializer<TSnapshot> snapshotDeserializer();

  TSnapshot toSnapshot(TCollector collector);

  TCollector fromSnapshot(TSnapshot snapshot);
}
