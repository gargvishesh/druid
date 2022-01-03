/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.result;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;

/**
 * This is a class that is meant to touch the last update time of a async query in order to allow it to not go away
 * while reading the results
 */
public class AsyncQueryResettingFilterInputStream extends FilterInputStream
{
  private final Runnable touchLastUpdateTime;

  private final long minimumElapsedReadTimeToReset;
  private long lastResetTime;
  private final Clock clock;

  public AsyncQueryResettingFilterInputStream(
      InputStream inputStream,
      Runnable touchLastUpdateTime,
      long minimumElapsedReadTimeToReset,
      Clock clock
  )
  {
    super(inputStream);
    this.touchLastUpdateTime = touchLastUpdateTime;
    this.minimumElapsedReadTimeToReset = minimumElapsedReadTimeToReset;
    this.clock = clock;
    this.lastResetTime = clock.millis();
  }

  @Override
  public void close() throws IOException
  {
    touchLastUpdateTime.run();
    super.close();
  }

  @Override
  public int read() throws IOException
  {
    touchLastUpdateTimeIfNecessary();
    return super.read();
  }

  private void touchLastUpdateTimeIfNecessary()
  {
    long currentTime = clock.millis();
    if (currentTime - lastResetTime >= minimumElapsedReadTimeToReset) {
      touchLastUpdateTime.run();
      lastResetTime = currentTime;
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException
  {
    touchLastUpdateTimeIfNecessary();
    return super.read(b, off, len);
  }

}
