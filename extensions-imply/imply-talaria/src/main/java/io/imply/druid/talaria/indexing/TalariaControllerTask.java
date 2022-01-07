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
import com.google.common.base.Preconditions;
import com.google.inject.Injector;
import io.imply.druid.talaria.exec.Leader;
import io.imply.druid.talaria.exec.LeaderContext;
import io.imply.druid.talaria.exec.LeaderImpl;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TimeChunkLockTryAcquireAction;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexTuningConfig;
import org.apache.druid.java.util.common.Intervals;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

@JsonTypeName(TalariaControllerTask.TYPE)
public class TalariaControllerTask extends AbstractTask
{
  public static final String TYPE = "talaria0";
  public static final String DUMMY_DATASOURCE_FOR_SELECT = "__you_have_been_visited_by_talaria";

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

  // TODO(gianm): HACK HACK HACK
  @JacksonInject
  private Injector injector;

  private volatile Leader leader;

  @JsonCreator
  public TalariaControllerTask(
      @JsonProperty("id") @Nullable String id,
      @JsonProperty("spec") TalariaQuerySpec querySpec,
      @JsonProperty("sqlQuery") @Nullable String sqlQuery,
      @JsonProperty("sqlQueryContext") @Nullable Map<String, Object> sqlQueryContext,
      @JsonProperty("sqlTypeNames") @Nullable List<String> sqlTypeNames,
      @JsonProperty("context") @Nullable Map<String, Object> context
  )
  {
    super(
        getOrMakeId(id, TYPE, getDataSourceForTaskMetadata(Preconditions.checkNotNull(querySpec, "querySpec"))),
        id,
        null,
        getDataSourceForTaskMetadata(querySpec),
        context
    );

    this.querySpec = querySpec;
    this.sqlQuery = sqlQuery;
    this.sqlQueryContext = sqlQueryContext;
    this.sqlTypeNames = sqlTypeNames;

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
    if (isIngestion(querySpec)) {
      // TODO(gianm): Don't require lock on ETERNITY
      return taskActionClient.submit(
          new TimeChunkLockTryAcquireAction(TaskLockType.EXCLUSIVE, Intervals.ETERNITY)
      ) != null;
    } else {
      return true;
    }
  }

  @Override
  public TaskStatus run(final TaskToolbox toolbox) throws Exception
  {
    LeaderContext context = new IndexerLeaderContext(toolbox, injector);
    leader = new LeaderImpl(this, context);
    return leader.run();
  }

  @Override
  public void stopGracefully(final TaskConfig taskConfig)
  {
    if (leader != null) {
      leader.stopGracefully();
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
    final TalariaDestination destination = querySpec.getDestination();

    if (destination instanceof DataSourceTalariaDestination) {
      return ((DataSourceTalariaDestination) destination).getDataSource();
    } else {
      return DUMMY_DATASOURCE_FOR_SELECT;
    }
  }

  public static boolean isIngestion(final TalariaQuerySpec querySpec)
  {
    return querySpec.getDestination() instanceof DataSourceTalariaDestination;
  }

  public boolean isWritable()
  {
    return isIngestion(querySpec);
  }
}
