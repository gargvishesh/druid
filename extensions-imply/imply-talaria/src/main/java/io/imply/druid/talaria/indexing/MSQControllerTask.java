/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.imply.druid.talaria.exec.Leader;
import io.imply.druid.talaria.exec.LeaderContext;
import io.imply.druid.talaria.exec.LeaderImpl;
import io.imply.druid.talaria.exec.TalariaTasks;
import io.imply.druid.talaria.util.TalariaContext;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@JsonTypeName(MSQControllerTask.TYPE)
public class MSQControllerTask extends AbstractTask
{
  public static final String TYPE = "query_controller";
  public static final String DUMMY_DATASOURCE_FOR_SELECT = "__query_select";

  private final TalariaQuerySpec querySpec;

  // Enables users, and the web console, to see the original SQL query (if any). Not used by anything else in Druid.
  @Nullable
  private final String sqlQuery;

  // Enables users, and the web console, to see the original SQL context (if any). Not used by any other Druid logic.
  @Nullable
  private final Map<String, Object> sqlQueryContext;

  // Enables users, and the web console, to see the original SQL type names (if any). Not used by any other Druid logic.
  @Nullable
  private final List<String> sqlTypeNames;
  @Nullable
  private final ExecutorService remoteFetchExecutorService;

  // TODO(gianm): HACK HACK HACK
  @JacksonInject
  private Injector injector;

  private volatile Leader leader;

  @JsonCreator
  public MSQControllerTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("spec") TalariaQuerySpec querySpec,
      @JsonProperty("sqlQuery") @Nullable String sqlQuery,
      @JsonProperty("sqlQueryContext") @Nullable Map<String, Object> sqlQueryContext,
      @JsonProperty("sqlTypeNames") @Nullable List<String> sqlTypeNames,
      @JsonProperty("context") @Nullable Map<String, Object> context
  )
  {
    super(
        id != null ? id : TalariaTasks.controllerTaskId(null),
        id,
        null,
        getDataSourceForTaskMetadata(querySpec),
        context
    );

    this.querySpec = querySpec;
    this.sqlQuery = sqlQuery;
    this.sqlQueryContext = sqlQueryContext;
    this.sqlTypeNames = sqlTypeNames;
    this.remoteFetchExecutorService = TalariaContext.isDurableStorageEnabled(querySpec.getQuery().getContext())
                                      ? Executors.newCachedThreadPool(Execs.makeThreadFactory(getId()
                                                                                              + "-remote-fetcher-%d"))
                                      : null;

    addToContext(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, true);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty("spec")
  public TalariaQuerySpec getQuerySpec()
  {
    return querySpec;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSqlQuery()
  {
    return sqlQuery;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, Object> getSqlQueryContext()
  {
    return sqlQueryContext;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getSqlTypeNames()
  {
    return sqlTypeNames;
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient) throws Exception
  {
    // If we're in replace mode, acquire locks for all intervals before declaring the task ready.
    if (isIngestion(querySpec) && ((DataSourceMSQDestination) querySpec.getDestination()).isReplaceTimeChunks()) {
      final List<Interval> intervals =
          ((DataSourceMSQDestination) querySpec.getDestination()).getReplaceTimeChunks();

      for (final Interval interval : intervals) {
        final TaskLock taskLock =
            taskActionClient.submit(new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, interval));

        if (taskLock == null) {
          return false;
        } else if (taskLock.isRevoked()) {
          throw new ISE(StringUtils.format("Lock for interval [%s] was revoked", interval));
        }
      }
    }

    return true;
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    final ServiceClientFactory clientFactory =
        injector.getInstance(Key.get(ServiceClientFactory.class, EscalatedGlobal.class));
    final OverlordClient overlordClient = injector.getInstance(OverlordClient.class);
    final LeaderContext context = new IndexerLeaderContext(
        toolbox,
        injector,
        clientFactory,
        overlordClient
    );
    leader = new LeaderImpl(this, context);
    return leader.run();
  }

  @Override
  public void stopGracefully(final TaskConfig taskConfig)
  {
    if (leader != null) {
      leader.stopGracefully();
    }
    if (remoteFetchExecutorService != null) {
      // This is to make sure we donot leak connections.
      remoteFetchExecutorService.shutdownNow();
    }
  }

  public ParallelIndexTuningConfig getTuningConfig()
  {
    return querySpec.getTuningConfig();
  }

  /**
   * TODO(gianm): Hack, because tasks must be associated with a datasource.
   */
  private static String getDataSourceForTaskMetadata(final TalariaQuerySpec querySpec)
  {
    final MSQDestination destination = querySpec.getDestination();

    if (destination instanceof DataSourceMSQDestination) {
      return ((DataSourceMSQDestination) destination).getDataSource();
    } else {
      return DUMMY_DATASOURCE_FOR_SELECT;
    }
  }

  public static boolean isIngestion(final TalariaQuerySpec querySpec)
  {
    return querySpec.getDestination() instanceof DataSourceMSQDestination;
  }
}
