/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.metrics;

import io.imply.druid.query.samplinggroupby.SamplingGroupByQuery;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.groupby.GroupByQuery;

public interface SamplingGroupByQueryMetrics extends QueryMetrics<SamplingGroupByQuery>
{
  /**
   * Sets the size of {@link SamplingGroupByQuery#getDimensions()} of the given query as dimension.
   */
  void numDimensions(SamplingGroupByQuery query);

  /**
   * Sets the number of metrics of the given groupBy query as dimension.
   */
  void numMetrics(SamplingGroupByQuery query);

  /**
   * Sets the number of "complex" metrics of the given groupBy query as dimension. By default it is assumed that
   * "complex" metric is a metric of not long or double type, but it could be redefined in the implementation of this
   * method.
   */
  void numComplexMetrics(SamplingGroupByQuery query);

  /**
   * Sets the granularity of {@link GroupByQuery#getGranularity()} of the given query as dimension.
   */
  void granularity(SamplingGroupByQuery query);
}
