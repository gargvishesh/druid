/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.imply.druid.talaria.frame.processor.Bouncer;
import io.imply.druid.talaria.indexing.MSQControllerTask;
import io.imply.druid.talaria.indexing.MSQSegmentGeneratorFrameProcessorFactory;
import io.imply.druid.talaria.indexing.TalariaWorkerTask;
import io.imply.druid.talaria.indexing.error.BroadcastTablesTooLargeFault;
import io.imply.druid.talaria.indexing.error.CanceledFault;
import io.imply.druid.talaria.indexing.error.CannotParseExternalDataFault;
import io.imply.druid.talaria.indexing.error.ColumnTypeNotSupportedFault;
import io.imply.druid.talaria.indexing.error.DurableStorageConfigurationFault;
import io.imply.druid.talaria.indexing.error.InsertCannotAllocateSegmentFault;
import io.imply.druid.talaria.indexing.error.InsertCannotBeEmptyFault;
import io.imply.druid.talaria.indexing.error.InsertCannotOrderByDescendingFault;
import io.imply.druid.talaria.indexing.error.InsertCannotReplaceExistingSegmentFault;
import io.imply.druid.talaria.indexing.error.InsertLockPreemptedFault;
import io.imply.druid.talaria.indexing.error.InsertTimeNullFault;
import io.imply.druid.talaria.indexing.error.InsertTimeOutOfBoundsFault;
import io.imply.druid.talaria.indexing.error.InvalidNullByteFault;
import io.imply.druid.talaria.indexing.error.NotEnoughMemoryFault;
import io.imply.druid.talaria.indexing.error.QueryNotSupportedFault;
import io.imply.druid.talaria.indexing.error.RowTooLargeFault;
import io.imply.druid.talaria.indexing.error.TalariaFault;
import io.imply.druid.talaria.indexing.error.TaskStartTimeoutFault;
import io.imply.druid.talaria.indexing.error.TooManyBucketsFault;
import io.imply.druid.talaria.indexing.error.TooManyColumnsFault;
import io.imply.druid.talaria.indexing.error.TooManyInputFilesFault;
import io.imply.druid.talaria.indexing.error.TooManyPartitionsFault;
import io.imply.druid.talaria.indexing.error.TooManyWarningsFault;
import io.imply.druid.talaria.indexing.error.TooManyWorkersFault;
import io.imply.druid.talaria.indexing.error.UnknownFault;
import io.imply.druid.talaria.indexing.error.WorkerFailedFault;
import io.imply.druid.talaria.indexing.error.WorkerRpcFailedFault;
import io.imply.druid.talaria.indexing.externalsink.LocalTalariaExternalSink;
import io.imply.druid.talaria.indexing.externalsink.LocalTalariaExternalSinkConfig;
import io.imply.druid.talaria.indexing.externalsink.NilTalariaExternalSink;
import io.imply.druid.talaria.indexing.externalsink.TalariaExternalSink;
import io.imply.druid.talaria.indexing.externalsink.TalariaExternalSinkFrameProcessorFactory;
import io.imply.druid.talaria.indexing.report.TalariaTaskReport;
import io.imply.druid.talaria.kernel.NilExtraInfoHolder;
import io.imply.druid.talaria.querykit.InputStageDataSource;
import io.imply.druid.talaria.querykit.NilInputSource;
import io.imply.druid.talaria.querykit.common.OffsetLimitFrameProcessorFactory;
import io.imply.druid.talaria.querykit.groupby.GroupByPostShuffleFrameProcessorFactory;
import io.imply.druid.talaria.querykit.groupby.GroupByPreShuffleFrameProcessorFactory;
import io.imply.druid.talaria.querykit.scan.ScanQueryFrameProcessorFactory;
import io.imply.druid.talaria.util.PassthroughAggregatorFactory;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.DruidProcessingConfig;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TalariaIndexingModule implements DruidModule
{
  private static final String BASE_TALARIA_KEY = "druid.msq";
  public static final String TALARIA_EXTERNAL_SINK = String.join(".", BASE_TALARIA_KEY, "externalsink");
  public static final String TALARIA_EXTERNAL_SINK_TYPE = String.join(".", TALARIA_EXTERNAL_SINK, "type");
  public static final String TALARIA_INTERMEDIATE_STORAGE = String.join(".", BASE_TALARIA_KEY, "intermediate.storage");
  public static final String TALARIA_INTERMEDIATE_STORAGE_TYPE = String.join(".", TALARIA_INTERMEDIATE_STORAGE, "type");

  public static final String TALARIA_INTERMEDIATE_STORAGE_ENABLED = String.join(
      ".",
      TALARIA_INTERMEDIATE_STORAGE,
      "enable"
  );

  public static final List<Class<? extends TalariaFault>> FAULT_CLASSES = ImmutableList.of(
      BroadcastTablesTooLargeFault.class,
      CanceledFault.class,
      CannotParseExternalDataFault.class,
      ColumnTypeNotSupportedFault.class,
      DurableStorageConfigurationFault.class,
      InsertCannotAllocateSegmentFault.class,
      InsertCannotBeEmptyFault.class,
      InsertCannotOrderByDescendingFault.class,
      InsertCannotReplaceExistingSegmentFault.class,
      InsertLockPreemptedFault.class,
      InsertTimeNullFault.class,
      InsertTimeOutOfBoundsFault.class,
      InvalidNullByteFault.class,
      NotEnoughMemoryFault.class,
      QueryNotSupportedFault.class,
      RowTooLargeFault.class,
      TaskStartTimeoutFault.class,
      TooManyBucketsFault.class,
      TooManyColumnsFault.class,
      TooManyInputFilesFault.class,
      TooManyPartitionsFault.class,
      TooManyWarningsFault.class,
      TooManyWorkersFault.class,
      UnknownFault.class,
      WorkerFailedFault.class,
      WorkerRpcFailedFault.class
  );

  @Override
  public List<? extends Module> getJacksonModules()
  {
    final SimpleModule module = new SimpleModule(getClass().getSimpleName());

    module.registerSubtypes(
        // Task classes
        MSQControllerTask.class,
        TalariaWorkerTask.class,

        // FrameChannelWorkerFactory and FrameChannelWorkerFactoryExtraInfoHolder classes
        MSQSegmentGeneratorFrameProcessorFactory.class,
        MSQSegmentGeneratorFrameProcessorFactory.SegmentGeneratorExtraInfoHolder.class,
        TalariaExternalSinkFrameProcessorFactory.class,
        ScanQueryFrameProcessorFactory.class,
        GroupByPreShuffleFrameProcessorFactory.class,
        GroupByPostShuffleFrameProcessorFactory.class,
        OffsetLimitFrameProcessorFactory.class,
        NilExtraInfoHolder.class,

        // FrameChannelWorkerFactory and FrameChannelWorkerFactoryExtraInfoHolder classes
        TalariaExternalSinkFrameProcessorFactory.class,
        ScanQueryFrameProcessorFactory.class,
        GroupByPreShuffleFrameProcessorFactory.class,
        GroupByPostShuffleFrameProcessorFactory.class,
        OffsetLimitFrameProcessorFactory.class,
        NilExtraInfoHolder.class,

        // DataSource classes (note: ExternalDataSource is in TalariaSqlModule)
        InputStageDataSource.class,

        // TaskReport classes
        TalariaTaskReport.class,

        // Other
        PassthroughAggregatorFactory.class,
        NilInputSource.class
    );

    FAULT_CLASSES.forEach(module::registerSubtypes);

    return Collections.singletonList(module);
  }

  @Override
  public void configure(Binder binder)
  {
    PolyBind.createChoice(
        binder,
        TALARIA_EXTERNAL_SINK_TYPE,
        Key.get(TalariaExternalSink.class),
        Key.get(NilTalariaExternalSink.class)
    );

    PolyBind.optionBinder(binder, Key.get(TalariaExternalSink.class))
            .addBinding(NilTalariaExternalSink.TYPE)
            .to(NilTalariaExternalSink.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(TalariaExternalSink.class))
            .addBinding(LocalTalariaExternalSink.TYPE)
            .to(LocalTalariaExternalSink.class)
            .in(LazySingleton.class);

    JsonConfigProvider.bind(binder, TALARIA_EXTERNAL_SINK, LocalTalariaExternalSinkConfig.class);
  }

  @Provides
  @LazySingleton
  public Bouncer makeBouncer(final DruidProcessingConfig processingConfig, @Self final Set<NodeRole> nodeRoles)
  {
    if (nodeRoles.contains(NodeRole.PEON) && !nodeRoles.contains(NodeRole.INDEXER)) {
      // CliPeon -> use only one thread regardless of configured # of processing threads. This matches the expected
      // resource usage pattern for CliPeon-based tasks (one task / one working thread per JVM).
      return new Bouncer(1);
    } else {
      return new Bouncer(processingConfig.getNumThreads());
    }
  }
}
