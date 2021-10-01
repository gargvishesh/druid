/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.tests.async;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails.State;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetailsApiResponse;
import io.imply.druid.tests.ImplyTestNGGroup;
import io.imply.druid.tests.client.AsyncResourceTestClient;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.QueryResultVerifier;
import org.apache.druid.testing.utils.SqlQueryWithResults;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.tests.indexer.AbstractS3InputSourceParallelIndexTest;
import org.junit.Assert;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@Test(groups = ImplyTestNGGroup.IMPLY_S3)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITS3SqlAsyncResultManager extends AbstractS3InputSourceParallelIndexTest
{
  private static final Logger LOG = new Logger(ITS3SqlAsyncResultManager.class);
  private static final String INDEX_TASK = "/indexer/wikipedia_local_input_source_index_task.json";
  private static final String IMPLY_SQL_ASYNC_TEST_QUERIES = "/queries/sql_async_queries.json";
  private static final InputFormatDetails JSON_FORMAT_DETAILS = InputFormatDetails.JSON;

  @Inject
  private AsyncResourceTestClient testClient;

  @Inject
  private ObjectMapper jsonMapper;

  @Test(timeOut = 300_000)
  public void doTest() throws Exception
  {
    final String dataSource = loadData();
    try (final Closeable ignored = unloader(dataSource)) {
      boolean failed = false;
      for (SqlQueryWithResults sqlQueryWithResults : loadTestQueries()) {
        final SqlQuery query = updateDataSource(sqlQueryWithResults.getQuery(), dataSource);
        LOG.info("Running Query %s", query);
        SqlAsyncQueryDetailsApiResponse response = testClient.submitAsyncQuery(query);
        Assert.assertEquals(response.getState(), State.INITIALIZED);
        final String asyncResultId = response.getAsyncResultId();

        ITRetryUtil.retryUntilTrue(
            () -> testClient.getStatus(asyncResultId).getState().isFinal(),
            "Waiting for query to complete"
        );

        Assert.assertEquals(testClient.getStatus(asyncResultId).getState(), State.COMPLETE);
        List<Map<String, Object>> results = testClient.getResults(asyncResultId);
        final boolean correctResults = QueryResultVerifier.compareResults(
            results,
            sqlQueryWithResults.getExpectedResults(),
            sqlQueryWithResults.getFieldsToTest()
        );
        if (!correctResults) {
          LOG.error(
              "Failed while executing query %s \n expectedResults: %s \n actualResults : %s",
              query,
              jsonMapper.writeValueAsString(sqlQueryWithResults.getExpectedResults()),
              jsonMapper.writeValueAsString(results)
          );
          failed = true;
        } else {
          LOG.info("Results Verified for Query %s", query);
        }
      }

      if (failed) {
        throw new ISE("one or more queries failed");
      }
    }
  }

  private String loadData() throws IOException
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID() + config.getExtraDatasourceNameSuffix();

    final Function<String, String> s3PropsTransform = spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%DATASOURCE%%",
            indexDatasource
        );
        spec = StringUtils.replace(
            spec,
            "%%PARTITIONS_SPEC%%",
            jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_SOURCE_FILTER%%",
            "*"
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_SOURCE_BASE_DIR%%",
            "/resources/data/batch_index" + JSON_FORMAT_DETAILS.getFolderSuffix()
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_FORMAT%%",
            jsonMapper.writeValueAsString(
                ImmutableMap.of(
                    "type",
                    JSON_FORMAT_DETAILS.getInputFormatType()
                )
            )
        );
        spec = StringUtils.replace(
            spec,
            "%%APPEND_TO_EXISTING%%",
            jsonMapper.writeValueAsString(false)
        );
        spec = StringUtils.replace(
            spec,
            "%%DROP_EXISTING%%",
            jsonMapper.writeValueAsString(false)
        );
        spec = StringUtils.replace(
            spec,
            "%%FORCE_GUARANTEED_ROLLUP%%",
            jsonMapper.writeValueAsString(false)
        );
        return spec;

      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    final String taskSpec = s3PropsTransform.apply(getResourceAsString(INDEX_TASK));
    LOG.info(taskSpec);

    submitTaskAndWait(
        taskSpec,
        indexDatasource,
        false,
        true,
        Pair.of(false, false)
    );

    return indexDatasource;
  }

  private SqlQuery updateDataSource(SqlQuery query, String dataSource)
  {
    return new SqlQuery(
        StringUtils.replace(
            query.getQuery(),
            "%%DATASOURCE%%",
            dataSource
        ),
        query.getResultFormat(),
        query.includeHeader(),
        query.getContext(),
        query.getParameters()
    );
  }

  private List<SqlQueryWithResults> loadTestQueries() throws IOException
  {
    return jsonMapper.readValue(
        TestQueryHelper.class.getResourceAsStream(IMPLY_SQL_ASYNC_TEST_QUERIES),
        new TypeReference<List<SqlQueryWithResults>>()
        {
        }
    );
  }
}
