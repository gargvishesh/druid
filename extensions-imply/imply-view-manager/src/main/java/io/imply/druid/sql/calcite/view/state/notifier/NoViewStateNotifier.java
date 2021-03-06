/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.notifier;

public class NoViewStateNotifier implements ViewStateNotifier
{
  @Override
  public void propagateViews(byte[] updatedViewMap)
  {
    throw new UnsupportedOperationException("Non-broker does not support propagateViews");
  }
}
