/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.google.common.base.Preconditions;
import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ShuffleSpecFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.Query;

import java.util.Map;

@SuppressWarnings("rawtypes")
public class MultiQueryKit implements QueryKit<Query<?>>
{
  private final Map<Class<? extends Query>, QueryKit> toolKitMap;

  public MultiQueryKit(final Map<Class<? extends Query>, QueryKit> toolKitMap)
  {
    this.toolKitMap = Preconditions.checkNotNull(toolKitMap, "toolKitMap");
  }

  @Override
  public QueryDefinition makeQueryDefinition(
      String queryId,
      Query<?> query,
      QueryKit<Query<?>> toolKitForSubQueries,
      ShuffleSpecFactory resultShuffleSpecFactory,
      int maxWorkerCount,
      int minStageNumber
  )
  {
    final QueryKit specificToolKit = toolKitMap.get(query.getClass());

    if (specificToolKit != null) {
      //noinspection unchecked
      return specificToolKit.makeQueryDefinition(
          queryId,
          query,
          this,
          resultShuffleSpecFactory,
          maxWorkerCount,
          minStageNumber
      );
    } else {
      throw new ISE("Unsupported query class [%s]", query.getClass().getName());
    }
  }
}
