/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.query;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.curator.discovery.ServerDiscoveryFactory;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.coordinator.rules.ForeverBroadcastDistributionRule;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.apache.druid.tests.query.ITWikipediaQueryTest;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

@Test(groups = TestNGGroup.QUERY)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITIndexedTableJoinQueryTest extends AbstractIndexerTest
{
  private static final String JOIN_TASK = "/indexer/indexed_table_loader_join_index_task.json";
  private static final String JOIN_METADATA_QUERIES_RESOURCE = "/queries/indexed_table_loader_join_metadata_queries.json";
  private static final String JOIN_QUERIES_RESOURCE = "/queries/indexed_table_loader_join_queries.json";
  private static final String JOIN_DATASOURCE = "indexed_table_loader_join_wikipedia_test";


  @Inject
  ServerDiscoveryFactory factory;

  @Inject
  CoordinatorResourceTestClient coordinatorClient;

  @Inject
  SqlTestQueryHelper queryHelper;

  @Inject
  @TestClient
  HttpClient httpClient;

  @Inject
  IntegrationTestingConfig config;

  @Test
  public void testJoin() throws Exception
  {
    final Closer closer = Closer.create();
    try {
      closer.register(unloader(JOIN_DATASOURCE));
      closer.register(() -> {
        // remove broadcast rule
        try {
          coordinatorClient.postLoadRules(
              JOIN_DATASOURCE,
              ImmutableList.of()
          );
        }
        catch (Exception ignored) {
        }
      });

      // prepare for broadcast
      coordinatorClient.postLoadRules(
          JOIN_DATASOURCE,
          ImmutableList.of(new ForeverBroadcastDistributionRule())
      );

      // load the data
      String taskJson = replaceJoinTemplate(getResourceAsString(JOIN_TASK), JOIN_DATASOURCE);
      indexer.submitTask(taskJson);

      ITRetryUtil.retryUntilTrue(
          () -> coordinatorClient.areSegmentsLoaded(JOIN_DATASOURCE), "broadcast segment load"
      );

      // query metadata until druid schema is refreshed and datasource is available joinable
      ITRetryUtil.retryUntilTrue(
          () -> {
            try {
              queryHelper.testQueriesFromString(
                  queryHelper.getQueryURL(config.getRouterUrl()),
                  replaceJoinTemplate(
                      getResourceAsString(JOIN_METADATA_QUERIES_RESOURCE),
                      JOIN_DATASOURCE
                  )
              );
              return true;
            }
            catch (Exception ex) {
              return false;
            }
          },
          "waiting for SQL metadata refresh"
      );

      // now do some queries
      queryHelper.testQueriesFromString(
          queryHelper.getQueryURL(config.getRouterUrl()),
          replaceJoinTemplate(getResourceAsString(JOIN_QUERIES_RESOURCE), JOIN_DATASOURCE)
      );
    }
    finally {
      closer.close();
    }
  }

  private static String replaceJoinTemplate(String template, String joinDataSource)
  {
    return StringUtils.replace(
        StringUtils.replace(template, "%%JOIN_DATASOURCE%%", joinDataSource),
        "%%REGULAR_DATASOURCE%%",
        ITWikipediaQueryTest.WIKIPEDIA_DATA_SOURCE
    );
  }
}
