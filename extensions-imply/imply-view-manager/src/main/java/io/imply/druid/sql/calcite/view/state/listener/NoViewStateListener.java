/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.listener;

import io.imply.druid.sql.calcite.view.ImplyViewDefinition;

import java.util.Map;

public class NoViewStateListener implements ViewStateListener
{
  @Override
  public void setViewState(byte[] serializedViewState)
  {
    throw new UnsupportedOperationException(
        "setViewState not supported"
    );
  }

  @Override
  public Map<String, ImplyViewDefinition> getViewState()
  {
    throw new UnsupportedOperationException(
        "getViewState not supported"
    );
  }
}
