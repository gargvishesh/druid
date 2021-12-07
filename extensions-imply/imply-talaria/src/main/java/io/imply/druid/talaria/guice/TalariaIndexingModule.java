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
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provides;
import io.imply.druid.license.ImplyLicenseManager;
import io.imply.druid.talaria.frame.processor.Bouncer;
import io.imply.druid.talaria.indexing.TalariaControllerTask;
import io.imply.druid.talaria.indexing.TalariaResultsTaskReport;
import io.imply.druid.talaria.indexing.TalariaSegmentGeneratorFrameProcessorFactory;
import io.imply.druid.talaria.indexing.TalariaStagesTaskReport;
import io.imply.druid.talaria.indexing.TalariaStatusTaskReport;
import io.imply.druid.talaria.indexing.TalariaWorkerTask;
import io.imply.druid.talaria.indexing.error.CanceledFault;
import io.imply.druid.talaria.indexing.error.CannotParseExternalDataFault;
import io.imply.druid.talaria.indexing.error.ColumnTypeNotSupportedFault;
import io.imply.druid.talaria.indexing.error.InsertCannotAllocateSegmentFault;
import io.imply.druid.talaria.indexing.error.InsertCannotBeEmptyFault;
import io.imply.druid.talaria.indexing.error.InsertCannotOrderByDescendingFault;
import io.imply.druid.talaria.indexing.error.InsertCannotReplaceExistingSegmentFault;
import io.imply.druid.talaria.indexing.error.InsertMissingComplexAggregatorFault;
import io.imply.druid.talaria.indexing.error.InsertTimeOutOfBoundsFault;
import io.imply.druid.talaria.indexing.error.QueryNotSupportedFault;
import io.imply.druid.talaria.indexing.error.RowTooLargeFault;
import io.imply.druid.talaria.indexing.error.TooManyBucketsFault;
import io.imply.druid.talaria.indexing.error.TooManyColumnsFault;
import io.imply.druid.talaria.indexing.error.TooManyInputFilesFault;
import io.imply.druid.talaria.indexing.error.TooManyPartitionsFault;
import io.imply.druid.talaria.indexing.error.UnknownFault;
import io.imply.druid.talaria.indexing.externalsink.LocalTalariaExternalSink;
import io.imply.druid.talaria.indexing.externalsink.LocalTalariaExternalSinkConfig;
import io.imply.druid.talaria.indexing.externalsink.NilTalariaExternalSink;
import io.imply.druid.talaria.indexing.externalsink.TalariaExternalSink;
import io.imply.druid.talaria.indexing.externalsink.TalariaExternalSinkFrameProcessorFactory;
import io.imply.druid.talaria.kernel.NilExtraInfoHolder;
import io.imply.druid.talaria.querykit.InputStageDataSource;
import io.imply.druid.talaria.querykit.NilInputSource;
import io.imply.druid.talaria.querykit.common.LimitFrameProcessorFactory;
import io.imply.druid.talaria.querykit.common.OrderByFrameProcessorFactory;
import io.imply.druid.talaria.querykit.groupby.GroupByPostShuffleFrameProcessorFactory;
import io.imply.druid.talaria.querykit.groupby.GroupByPreShuffleFrameProcessorFactory;
import io.imply.druid.talaria.querykit.scan.ScanQueryFrameProcessorFactory;
import io.imply.druid.talaria.util.PassthroughAggregatorFactory;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DruidProcessingConfig;

import java.util.Collections;
import java.util.List;

public class TalariaIndexingModule implements DruidModule
{
  @Inject
  ImplyLicenseManager implyLicenseManager = null;

  @Override
  public List<? extends Module> getJacksonModules()
  {
    if (!implyLicenseManager.isFeatureEnabled(TalariaModules.FEATURE_NAME)) {
      return Collections.emptyList();
    }

    return Collections.singletonList(
        new SimpleModule(getClass().getSimpleName()).registerSubtypes(
            // Task classes
            TalariaControllerTask.class,
            TalariaWorkerTask.class,

            // FrameChannelWorkerFactory and FrameChannelWorkerFactoryExtraInfoHolder classes
            TalariaSegmentGeneratorFrameProcessorFactory.class,
            TalariaSegmentGeneratorFrameProcessorFactory.SegmentGeneratorExtraInfoHolder.class,
            TalariaExternalSinkFrameProcessorFactory.class,
            ScanQueryFrameProcessorFactory.class,
            GroupByPreShuffleFrameProcessorFactory.class,
            GroupByPostShuffleFrameProcessorFactory.class,
            OrderByFrameProcessorFactory.class,
            LimitFrameProcessorFactory.class,
            NilExtraInfoHolder.class,

            // DataSource classes (note: ExternalDataSource is in TalariaSqlModule)
            InputStageDataSource.class,

            // TaskReport classes
            TalariaStagesTaskReport.class,
            TalariaStatusTaskReport.class,
            TalariaResultsTaskReport.class,

            // TalariaFault classes
            CanceledFault.class,
            CannotParseExternalDataFault.class,
            ColumnTypeNotSupportedFault.class,
            InsertCannotAllocateSegmentFault.class,
            InsertCannotBeEmptyFault.class,
            InsertCannotOrderByDescendingFault.class,
            InsertCannotReplaceExistingSegmentFault.class,
            InsertTimeOutOfBoundsFault.class,
            InsertMissingComplexAggregatorFault.class,
            QueryNotSupportedFault.class,
            RowTooLargeFault.class,
            TooManyBucketsFault.class,
            TooManyColumnsFault.class,
            TooManyInputFilesFault.class,
            TooManyPartitionsFault.class,
            UnknownFault.class,

            // Other
            PassthroughAggregatorFactory.class,
            NilInputSource.class
        )
    );
  }

  @Override
  public void configure(Binder binder)
  {
    if (!implyLicenseManager.isFeatureEnabled(TalariaModules.FEATURE_NAME)) {
      return;
    }

    PolyBind.createChoice(
        binder,
        "druid.talaria.externalsink.type",
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

    JsonConfigProvider.bind(binder, "druid.talaria.externalsink", LocalTalariaExternalSinkConfig.class);
  }

  @Provides
  @LazySingleton
  public Bouncer makeBouncer(final DruidProcessingConfig processingConfig)
  {
    if (!implyLicenseManager.isFeatureEnabled(TalariaModules.FEATURE_NAME)) {
      // This provider will not be used if none of the other Talaria stuff is bound, so this exception will
      // not actually be reached in production. But include it here anyway, to make it clear that it is only
      // expected to be used in concert with the rest of the extension.
      throw new ISE("Not used");
    }

    // TODO(gianm): do bouncer size = 1 on Peons?
    return new Bouncer(processingConfig.getNumThreads());
  }
}
