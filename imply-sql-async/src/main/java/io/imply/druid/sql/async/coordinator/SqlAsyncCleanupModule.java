/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import io.imply.druid.sql.async.SqlAsyncModule;
import io.imply.druid.sql.async.coordinator.duty.KillAsyncQueryMetadata;
import io.imply.druid.sql.async.coordinator.duty.KillAsyncQueryResultWithoutMetadata;
import io.imply.druid.sql.async.coordinator.duty.UpdateStaleQueryState;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class SqlAsyncCleanupModule implements DruidModule
{
  public static final String BASE_CLEANUP_CONFIG_KEY = String.join(".", SqlAsyncModule.BASE_ASYNC_CONFIG_KEY, "cleanup");
  public static final String CLEANUP_POLL_PERIOD_CONFIG_KEY = String.join(".", BASE_CLEANUP_CONFIG_KEY, "pollPeriod");
  public static final String CLEANUP_TIME_TO_RETAIN_CONFIG_KEY = String.join(".", BASE_CLEANUP_CONFIG_KEY, KillAsyncQueryMetadata.TIME_TO_RETAIN_KEY);
  public static final String TIME_TO_WAIT_AFTER_BROKER_GONE = String.join(
      ".",
      BASE_CLEANUP_CONFIG_KEY,
      UpdateStaleQueryState.TIME_TO_WAIT_AFTER_BROKER_GONE
  );

  private static final Logger LOG = new Logger(SqlAsyncCleanupModule.class);
  private static final String DEFAULT_QUERY_TIME_TO_RETAIN = "PT60S";
  private static final String DEFAULT_ASYNC_CLEANUP_COORDINATOR_DUTY_PERIOD = "PT30S";

  @Inject
  private Properties props;

  @Inject
  private ObjectMapper jsonMapper;

  public SqlAsyncCleanupModule()
  {
  }

  @VisibleForTesting
  SqlAsyncCleanupModule(Properties props, ObjectMapper jsonMapper)
  {
    this.props = props;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    if (SqlAsyncModule.isEnabled(props)) {
      return ImmutableList.<Module>of(
          new SimpleModule("SqlAsyncCleanupModule")
              .registerSubtypes(
                  KillAsyncQueryMetadata.class,
                  KillAsyncQueryResultWithoutMetadata.class,
                  UpdateStaleQueryState.class
              )
      );
    } else {
      return ImmutableList.of();
    }
  }

  @Override
  public void configure(Binder binder)
  {
    if (SqlAsyncModule.isEnabled(props)) {
      SqlAsyncModule.bindAsyncMetadataManager(binder);
      SqlAsyncModule.bindAsyncStorage(binder);
      SqlAsyncModule.bindAsyncLimitsConfig(binder);
      try {
        setupAsyncCleanupCoordinatorDutyGroup(jsonMapper, props);
      }
      catch (Exception e) {
        LOG.error(e, "Failed to setup Async coordinator cleanup duties");
      }
    }
  }

  private static void setupAsyncCleanupCoordinatorDutyGroup(ObjectMapper jsonMapper, Properties props) throws Exception
  {
    setupAsyncCleanupCoordinatorDutyGroup(
        StringUtils.format("asyncResultsCleanupInternal-%s", UUID.randomUUID()),
        jsonMapper,
        props
    );
  }

  @VisibleForTesting
  static void setupAsyncCleanupCoordinatorDutyGroup(
      String asyncCleanupGroupName,
      ObjectMapper jsonMapper,
      Properties props
  ) throws Exception
  {
    final String asyncCleanupPeriod;
    if (Strings.isNullOrEmpty(props.getProperty(CLEANUP_POLL_PERIOD_CONFIG_KEY))) {
      asyncCleanupPeriod = DEFAULT_ASYNC_CLEANUP_COORDINATOR_DUTY_PERIOD;
    } else {
      asyncCleanupPeriod = props.getProperty(CLEANUP_POLL_PERIOD_CONFIG_KEY);
    }
    final String asyncCleanupTimeToRetain;
    if (Strings.isNullOrEmpty(props.getProperty(CLEANUP_TIME_TO_RETAIN_CONFIG_KEY))) {
      asyncCleanupTimeToRetain = DEFAULT_QUERY_TIME_TO_RETAIN;
    } else {
      asyncCleanupTimeToRetain = props.getProperty(CLEANUP_TIME_TO_RETAIN_CONFIG_KEY);
    }
    final String timeToWaitAfterBrokerGone;
    if (Strings.isNullOrEmpty(props.getProperty(TIME_TO_WAIT_AFTER_BROKER_GONE))) {
      timeToWaitAfterBrokerGone = null;
    } else {
      timeToWaitAfterBrokerGone = props.getProperty(TIME_TO_WAIT_AFTER_BROKER_GONE);
    }

    // Added randomUUID to async cleanup group name to ensure no collision with any other user provided Coordinator Custom Duty groups
    if (Strings.isNullOrEmpty(props.getProperty("druid.coordinator.dutyGroups"))) {
      props.setProperty("druid.coordinator.dutyGroups", jsonMapper.writeValueAsString(ImmutableList.of(asyncCleanupGroupName)));
    } else {
      List<String> coordinatorCustomDutyGroupNames = jsonMapper.readValue(props.getProperty("druid.coordinator.dutyGroups"), new TypeReference<List<String>>() {});
      coordinatorCustomDutyGroupNames.add(asyncCleanupGroupName);
      props.setProperty("druid.coordinator.dutyGroups", jsonMapper.writeValueAsString(coordinatorCustomDutyGroupNames));
    }
    props.setProperty(
        StringUtils.format("druid.coordinator.%s.duties", asyncCleanupGroupName),
        jsonMapper.writeValueAsString(
            ImmutableList.of(
                KillAsyncQueryMetadata.JSON_TYPE_NAME,
                KillAsyncQueryResultWithoutMetadata.JSON_TYPE_NAME,
                UpdateStaleQueryState.TYPE
            )
        )
    );
    props.setProperty(
        StringUtils.format(
            "druid.coordinator.%s.duty.%s.%s",
            asyncCleanupGroupName,
            KillAsyncQueryMetadata.JSON_TYPE_NAME,
            KillAsyncQueryMetadata.TIME_TO_RETAIN_KEY
        ),
        asyncCleanupTimeToRetain
    );
    if (timeToWaitAfterBrokerGone != null) {
      props.setProperty(
          StringUtils.format(
              "druid.coordinator.%s.duty.%s.%s",
              asyncCleanupGroupName,
              UpdateStaleQueryState.TYPE,
              UpdateStaleQueryState.TIME_TO_WAIT_AFTER_BROKER_GONE
          ),
          timeToWaitAfterBrokerGone
      );
    }
    props.setProperty(
        StringUtils.format("druid.coordinator.%s.period", asyncCleanupGroupName),
        asyncCleanupPeriod
    );
  }
}
