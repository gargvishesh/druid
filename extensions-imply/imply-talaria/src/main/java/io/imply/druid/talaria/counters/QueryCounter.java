/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.counters;

import javax.annotation.Nullable;

/**
 * A kind of query counter. Counters are flexible in what they can track and how they work, so this interface
 * does not specify anything beyond requiring the ability to take a snapshot.
 */
public interface QueryCounter
{
  @Nullable
  QueryCounterSnapshot snapshot();
}
