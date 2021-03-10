/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.notifier;

public interface ViewStateNotifier
{
  /**
   * Send the view map state contained in updatedViewMap to all brokers
   *
   * @param updatedViewMap View map state
   */
  void propagateViews(byte[] updatedViewMap);
}
