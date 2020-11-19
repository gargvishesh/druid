/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest;

import io.airlift.airline.Cli;
import org.apache.druid.cli.CliCommandCreator;

public class IngestServiceCommandCreator implements CliCommandCreator
{
  @Override
  public void addCommands(Cli.CliBuilder builder)
  {
    builder.withGroup("server").withCommands(IngestService.class);
  }
}
