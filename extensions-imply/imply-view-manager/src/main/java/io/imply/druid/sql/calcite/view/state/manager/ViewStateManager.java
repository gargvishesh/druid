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

public interface ViewStateManager
{
  int createView(ImplyViewDefinition viewDefinition);

  int alterView(ImplyViewDefinition viewDefinition);

  int deleteView(String viewName, @Nullable String viewNamespace);

  Map<String, ImplyViewDefinition> getViewState();

  byte[] getViewStateSerialized();
}
