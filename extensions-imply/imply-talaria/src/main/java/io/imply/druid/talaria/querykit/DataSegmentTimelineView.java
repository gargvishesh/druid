/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.joda.time.Interval;

import java.util.List;
import java.util.Optional;

public interface DataSegmentTimelineView
{
  /**
   * Returns the timeline for a datasource, if it exists. The analysis object passed in must represent a scan-based
   * datasource of a single table. (i.e., {@link DataSourceAnalysis#getBaseTableDataSource()} must be present.)
   *
   * TODO(gianm): Define "exists"
   *
   * @param dataSource table data source name
   * @param intervals  relevant intervals. The returned timeline will *at least* include all segments that overlap
   *                   these intervals. It may also include more. Empty means the timeline may not contain any
   *                   segments at all.
   *
   * @return timeline, if it exists
   *
   * @throws IllegalStateException if 'analysis' does not represent a scan-based datasource of a single table
   */
  Optional<TimelineLookup<String, DataSegment>> getTimeline(
      String dataSource,
      List<Interval> intervals
  );
}
