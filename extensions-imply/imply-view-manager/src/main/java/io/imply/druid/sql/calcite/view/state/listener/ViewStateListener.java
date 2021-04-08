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

/**
 * Listens for changes in SQL views configuration, to ultimately update
 * {@link org.apache.druid.sql.calcite.view.ViewManager} with any changes to the set of views available to query
 */
public interface ViewStateListener
{
  void setViewState(byte[] serializedViewState);

  Map<String, ImplyViewDefinition> getViewState();
}
