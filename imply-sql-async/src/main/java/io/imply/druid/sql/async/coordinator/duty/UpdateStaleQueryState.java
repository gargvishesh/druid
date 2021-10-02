/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.coordinator.duty;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.imply.druid.sql.async.SqlAsyncUtil;
import io.imply.druid.sql.async.discovery.BrokerIdService;
import io.imply.druid.sql.async.exception.AsyncQueryDoesNotExistException;
import io.imply.druid.sql.async.metadata.SqlAsyncMetadataManager;
import io.imply.druid.sql.async.query.SqlAsyncQueryDetails;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.DruidService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.duty.CoordinatorCustomDuty;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is a special coordinator duty to handle stale query states caused by potential broker failures.
 * For async downloads, brokers are primarily responsible for updating query state,
 * but if they cannot for some reason, this duty will mark those queries as {@code UNDETERMINED},
 * indicating that the query failed because of the broker gone offline.
 *
 * This mechanism is implemented as a coordinator duty instead of being a service always running in the coordinator
 * because the coordinator watching brokers can fail as well as brokers. The mechanism to handle stale query states
 * should be called in a polling manner, so that we will not miss any stale states.
 *
 * Since it is hard to tell whether the broker has actually gone offline or cannot announce itself for some reason,
 * this class is very conservative and have defensive mechansims so that we will not fail queries too aggressively.
 * For example, when a missing broker is noticed, this duty waits for the next run instead of taking an action
 * immediately. In the next run, the duty will check whether the broker is still missing, and schedule a task
 * that will wait for another {@link #timeToWaitAfterBrokerGoneMs} and finally update stale query state.
 */
@JsonTypeName(UpdateStaleQueryState.TYPE)
public class UpdateStaleQueryState implements CoordinatorCustomDuty
{
  public static final String TYPE = "updateStaleQueryState";
  public static final String TIME_TO_WAIT_AFTER_BROKER_GONE = "timeToWaitAfterBrokerGone";

  static final String STALE_QUERIES_MARKED_UNDETERMINED_COUNT = "async/query/undetermined/count";

  private static final Duration DEFAULT_TIME_TO_WAIT = new Duration("PT60S");
  private static final EmittingLogger LOG = new EmittingLogger(UpdateStaleQueryState.class);

  private final long timeToWaitAfterBrokerGoneMs;
  private final SqlAsyncMetadataManager sqlAsyncMetadataManager;
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  /**
   * This lock coordinates 3 threads.
   *
   * - {@link DruidNodeDiscovery.Listener} executor executes callbacks for discovery events.
   *   This thread can update {@link #liveBrokers} and {@link #stateUpdateFutures}.
   * - Main thread runs this coordinator duty. This thread can read {@link #liveBrokers}
   *   and update {@link #stateUpdateFutures}.
   * - {@link #exec} executes scheduled tasks to update stale query states.
   *   This thread can update {@link #stateUpdateFutures}.
   */
  private final Object lock = new Object();

  @GuardedBy("lock")
  private final Set<String> liveBrokers = new HashSet<>();

  // brokerId -> futures handling stale states
  @GuardedBy("lock")
  private final Map<String, Map<String, Future<?>>> stateUpdateFutures = new HashMap<>();
  private final ScheduledExecutorService exec;
  private final AtomicInteger numStaleQueriesMarked = new AtomicInteger();

  private boolean firstRun = true;
  private int remainingRunsToWaitBrokerViewInitialized = 3;

  @JsonCreator
  public UpdateStaleQueryState(
      @JsonProperty(TIME_TO_WAIT_AFTER_BROKER_GONE) Duration timeToWaitAfterBrokerGone,
      @JacksonInject SqlAsyncMetadataManager sqlAsyncMetadataManager,
      @JacksonInject DruidNodeDiscoveryProvider druidNodeDiscoveryProvider
  )
  {
    this.timeToWaitAfterBrokerGoneMs = timeToWaitAfterBrokerGone == null
                                       ? DEFAULT_TIME_TO_WAIT.getMillis()
                                       : timeToWaitAfterBrokerGone.getMillis();
    this.sqlAsyncMetadataManager = sqlAsyncMetadataManager;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.exec = Execs.scheduledSingleThreaded("Stale-query-state-updater");
  }

  private void initializeBrokerView()
  {
    DruidNodeDiscovery druidNodeDiscovery = druidNodeDiscoveryProvider.getForNodeRole(NodeRole.BROKER);
    druidNodeDiscovery.registerListener(
        new DruidNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
          {
            nodes.forEach(
                (node) -> {
                  Optional<DruidService> maybeBrokerIdService = Optional.ofNullable(
                      node.getServices().get(BrokerIdService.NAME)
                  );
                  // If the broker has brokerIdService, it can support async APIs.
                  // When a live broker with brokerIdService is found, this listener will add it to liveBrokers.
                  // If this broker is coming back from intermittent disconnections to ZK, this duty might have
                  // scheduled futures to handle the previous event of this broker gone offline. If there are
                  // any futures, this listener will cancel all those futures as soon as the broker is proved
                  // to be alive.
                  maybeBrokerIdService.ifPresent(
                      druidService -> {
                        final String brokerId = ((BrokerIdService) druidService).getBrokerId();
                        synchronized (lock) {
                          liveBrokers.add(brokerId);
                          Map<String, Future<?>> futures = stateUpdateFutures.remove(brokerId);
                          if (futures != null) {
                            for (Entry<String, Future<?>> entry : futures.entrySet()) {
                              try {
                                entry.getValue().cancel(true);
                              }
                              catch (Exception e) {
                                LOG.warn(e, "Failed to cancel future for query[%s]", entry.getKey());
                              }
                            }
                          }
                        }
                      }
                  );
                }
            );
          }

          @Override
          public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
          {
            nodes.forEach(
                (node) -> {
                  Optional<DruidService> maybeBrokerIdService = Optional.ofNullable(
                      node.getServices().get(BrokerIdService.NAME)
                  );
                  // We found a broker that has been disconnected from ZK. Let's remove it from liveBrokers.
                  maybeBrokerIdService.ifPresent(
                      druidService -> {
                        synchronized (lock) {
                          // We don't schedule stateUpdateFuture here. It will be scheduled in the next coordinator run.
                          liveBrokers.remove(((BrokerIdService) druidService).getBrokerId());
                        }
                      }
                  );
                }
            );
          }
        }
    );
  }

  @Nullable
  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    // Ideally, the node discovery should be registered on the coordinator lifecycle,
    // so that only the leader can watch brokers.
    // This requires refactoring of the lifecycle registration in coordinator
    // or DruidNodeDiscovery to support deleting listeners.
    if (firstRun) {
      initializeBrokerView();
      firstRun = false;
    }

    // This is for the case where the coordinator starts up later after brokers start running.
    // The broker view should be initialized immediately,
    // but maybe the initialization could take some time for some reason..
    if (remainingRunsToWaitBrokerViewInitialized > 0) {
      LOG.info(
          "Still waiting for the broker view to initialize. Remaining wait runs[%s]",
          remainingRunsToWaitBrokerViewInitialized--
      );
      return params;
    }

    // brokerId -> list of asyncResultIds
    final Collection<String> asyncResultIds;
    try {
      asyncResultIds = sqlAsyncMetadataManager.getAllAsyncResultIds();
    }
    catch (Exception e) {
      LOG.warn(e, "Failed to get asyncResultIds. Skipping this run.");
      return params;
    }

    int scheduled = 0;
    for (String asyncResultId : asyncResultIds) {
      try {
        final Optional<SqlAsyncQueryDetails> maybeQueryDetails = sqlAsyncMetadataManager.getQueryDetails(asyncResultId);
        if (!maybeQueryDetails.isPresent()) {
          LOG.warn("Failed to get query details for query [%s]", asyncResultId);
          continue;
        }
        final SqlAsyncQueryDetails queryDetails = maybeQueryDetails.get();

        final String brokerId = SqlAsyncUtil.getBrokerIdFromAsyncResultId(asyncResultId);
        synchronized (lock) {
          // The query state can be stale if the state is a non-final but its broker is missing.
          // However, we don't want to be too aggressive here. We will wait for another timeToWaitAfterBrokerGoneMs
          // and finally update the query state.
          if (!liveBrokers.contains(brokerId) && !queryDetails.getState().isFinal()) {
            scheduleStaleStateHandling(
                brokerId,
                asyncResultId,
                queryDetails.toUndetermined(),
                timeToWaitAfterBrokerGoneMs
            );
            scheduled++;
            LOG.debug("query[%s] will be marked undetermined in [%s] ms", asyncResultId, timeToWaitAfterBrokerGoneMs);
          }
        }
      }
      catch (Exception e) {
        LOG.warn(e, "Failed to check state for query [%s]", asyncResultId);
      }
    }

    LOG.info("Scheduled [%s] tasks to update query states.", scheduled);

    params.getCoordinatorStats().addToGlobalStat(
        STALE_QUERIES_MARKED_UNDETERMINED_COUNT,
        numStaleQueriesMarked.getAndSet(0)
    );

    return params;
  }

  @GuardedBy("lock")
  private void scheduleStaleStateHandling(
      String brokerId,
      String asyncResultId,
      SqlAsyncQueryDetails undeterminedQueryDetails,
      long delay
  )
  {
    final Runnable task = () -> {
      removeFuture(brokerId, asyncResultId);
      // Since query state is updated after the future is removed from stateUpdateFutures,
      // it's possible that another task is scheduled to update the same query state.
      // This should be OK since the second task will be effectively no-op as
      // the query state will have been already updated.
      try {
        markUndetermined(asyncResultId, undeterminedQueryDetails);
      }
      catch (AsyncQueryDoesNotExistException e) {
        LOG.warn(e, "Query [%s] does not exist", asyncResultId);
      }
      catch (Exception e) {
        LOG.error(e, "Failed to update query state for query[%s]", asyncResultId);
        LOG.makeAlert(e, "Failed to update query state to undetermined")
           .addData("asyncResultId", asyncResultId)
           .emit();
      }
    };
    stateUpdateFutures.computeIfAbsent(brokerId, k -> new HashMap<>())
                      .putIfAbsent(asyncResultId, exec.schedule(task, delay, TimeUnit.MILLISECONDS));
  }

  private void removeFuture(String brokerId, String asyncResultId)
  {
    synchronized (lock) {
      final Map<String, Future<?>> futures = stateUpdateFutures.get(brokerId);
      if (futures != null) {
        futures.remove(asyncResultId);
        if (futures.isEmpty()) {
          stateUpdateFutures.remove(brokerId);
        }
      }
    }
  }

  private void markUndetermined(String asyncResultId, SqlAsyncQueryDetails undeterminedQueryDetails)
      throws IOException, AsyncQueryDoesNotExistException
  {
    // TODO: This is racy because, even though the broker was not in the live brokers set, it might be still alive.
    //       The update should fail if the broker is alive and has updated the state before coordinator does.
    //       This will be fixed when compareAndSwap() method is added for metadaaManager.
    sqlAsyncMetadataManager.updateQueryDetails(undeterminedQueryDetails);
    LOG.debug("Marked query [%s] as UNDETERMINED", asyncResultId);
    numStaleQueriesMarked.addAndGet(1);
  }
}
