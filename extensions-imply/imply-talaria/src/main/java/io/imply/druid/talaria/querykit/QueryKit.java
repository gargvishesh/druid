/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import io.imply.druid.talaria.kernel.QueryDefinition;
import io.imply.druid.talaria.kernel.ShuffleSpecFactory;
import org.apache.druid.query.Query;

public interface QueryKit<QueryType extends Query<?>>
{
  QueryDefinition makeQueryDefinition(
      String queryId,
      QueryType query,
      QueryKit<Query<?>> toolKitForSubQueries,
      ShuffleSpecFactory resultShuffleSpecFactory,
      int maxWorkerCount,
      int minStageNumber
  );
}
