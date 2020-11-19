/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest;

import com.google.common.collect.ImmutableList;
import com.google.inject.Module;
import io.airlift.airline.Command;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.List;

@Command(
    name = IngestService.SERVICE_NAME,
    description = "these pants are so saasy, you'll want to wear them forever"
)
public class IngestService extends ServerRunnable
{
  private static final Logger LOG = new Logger(IngestService.class);

  /**
   * we thought about calling this node the 'saasylord'... imo, we still might
   */
  public static final String SERVICE_NAME = "ingest-service";

  // todo: lets talk about ports sometime
  public static final int PORT = 9001;
  public static final int TLS_PORT = 9101;

  public IngestService()
  {
    super(LOG);
  }

  @Override
  protected List<? extends Module> getModules()
  {
    return ImmutableList.of(
        new IndexingServiceTuningConfigModule(),
        new IngestServiceModule()
    );
  }
}
