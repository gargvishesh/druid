/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.notifier;

import java.util.function.Supplier;

public interface ViewStateNotifier
{
  /**
   * Set the source of updated data for the notifier. The Supplier should give the most recent view of the
   * view state when get() is called.
   */
  void setUpdateSource(Supplier<byte[]> updateSource);

  /**
   * Inform the notifier that an update has occurred. This will cause the notifier to read the current state
   * from the updateSource and send out a notification.
   */
  void scheduleUpdate();
}
