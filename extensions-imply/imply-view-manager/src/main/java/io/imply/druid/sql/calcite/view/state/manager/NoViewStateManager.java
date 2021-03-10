/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.view.state.manager;

import io.imply.druid.sql.calcite.view.ImplyViewDefinition;

import javax.annotation.Nullable;
import java.util.Map;

public class NoViewStateManager implements ViewStateManager
{
  @Override
  public int createView(ImplyViewDefinition viewDefinition)
  {
    throw new UnsupportedOperationException("Non-broker does not support createView");
  }

  @Override
  public int alterView(ImplyViewDefinition viewDefinition)
  {
    throw new UnsupportedOperationException("Non-broker does not support alterView");

  }

  @Override
  public int deleteView(String viewName, @Nullable String viewNamespace)
  {
    throw new UnsupportedOperationException("Non-broker does not support deleteView");
  }

  @Override
  public Map<String, ImplyViewDefinition> getViewState()
  {
    throw new UnsupportedOperationException("Non-broker does not support getViewState");
  }

  @Override
  public byte[] getViewStateSerialized()
  {
    throw new UnsupportedOperationException("Non-broker does not support getViewStateSerialized");
  }
}
