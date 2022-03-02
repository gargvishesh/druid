/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * A module for deserialization of {@link KeyCollectorSnapshot}.
 */
public class KeyCollectorSnapshotDeserializerModule extends SimpleModule
{
  public KeyCollectorSnapshotDeserializerModule(final KeyCollectorFactory<?, ?> keyCollectorFactory)
  {
    addDeserializer(KeyCollectorSnapshot.class, keyCollectorFactory.snapshotDeserializer());
  }
}

