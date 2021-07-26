/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.tests.ImplyTestNGGroup;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.server.coordinator.rules.IntervalLoadRule;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = ImplyTestNGGroup.VIRTUAL_SEGMENTS)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITVirtualSegmentQueryTest extends AbstractIndexerTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_data_index_task.json";
  private static final String METADATA_QUERIES_RESOURCE = "/queries/wikipedia_metadata_queries.json";
  private static final String QUERIES_RESOURCE = "/queries/virtual_segment_queries.json";
  private static final String DATASOURCE = "virtual_segment_wikipedia_test";

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  @Inject
  SqlTestQueryHelper queryHelper;

  @Inject
  IntegrationTestingConfig config;

  @Test
  public void testQuery() throws Exception
  {
    try (Closer closer = Closer.create()) {
      closer.register(unloader(DATASOURCE));
      closer.register(() -> {
        // remove cold tier load rule
        try {
          coordinatorClient.postLoadRules(
              DATASOURCE,
              ImmutableList.of()
          );
        }
        catch (Exception ignored) {
        }
      });

      // add load rule for cold tier
      coordinatorClient.postLoadRules(
          DATASOURCE,
          ImmutableList.of(new IntervalLoadRule(Intervals.ETERNITY, ImmutableMap.of("__cold-tier", 1, DruidServer.DEFAULT_TIER, 0)))
      );

      // each segment is of 8385 bytes size. We submit task request for 4 segments
      String taskJson = StringUtils.replace(getResourceAsString(INDEX_TASK), "%%DATASOURCE%%", DATASOURCE);
      DateTime time = DateTime.now(DateTimeZone.UTC);
      for (int i = 0; i < 4; i++) {
        time = time.minusDays(1);
        String taskJsonWithTime = StringUtils.replace(taskJson, "%%DATE%%", time.toString());
        indexer.submitTask(taskJsonWithTime);

        int size = i + 1;
        ITRetryUtil.retryUntilTrue(
            () -> coordinatorClient.getAvailableSegments(DATASOURCE).size() >= size,
            "wikipedia data load for time " + time
        );
      }


      // Now we have more segments that can be stored on disk. so proceed with the query

      // query metadata until druid schema is refreshed and datasource is available joinable
      ITRetryUtil.retryUntilTrue(
          () -> {
            queryHelper.testQueriesFromString(
                queryHelper.getQueryURL(config.getRouterUrl()),
                StringUtils.replace(
                    getResourceAsString(METADATA_QUERIES_RESOURCE),
                    "%%TABLE%%",
                    DATASOURCE
                )
            );
            return true;
          },
          "waiting for SQL metadata refresh"
      );

      // now do some queries
      queryHelper.testQueriesFromString(
          queryHelper.getQueryURL(config.getRouterUrl()),
          StringUtils.replace(
              getResourceAsString(QUERIES_RESOURCE),
              "%%TABLE%%",
              DATASOURCE
          )
      );
    }
  }
}
