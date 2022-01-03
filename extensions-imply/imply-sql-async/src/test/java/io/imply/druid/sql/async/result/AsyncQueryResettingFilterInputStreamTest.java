/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.result;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Clock;
import java.util.Locale;

@RunWith(MockitoJUnitRunner.class)
public class AsyncQueryResettingFilterInputStreamTest
{
  private File file;
  private TestRunnable testRunnable;
  @Mock
  private Clock clock;

  private static final long CLOCK_START_TIME = 100L;
  private long currentTime = CLOCK_START_TIME;
  private static final long ASYNC_LAST_UPDATE_TRIGGER_TIME = 31L;

  @Before
  public void setUp()
  {
    file = new File(
        "src/test/java/io/imply/druid/sql/async/result/AsyncQueryResettingFilterInputStreamTest.java");
    testRunnable = new TestRunnable();
    Mockito.when(clock.millis()).thenReturn(CLOCK_START_TIME);
    currentTime = CLOCK_START_TIME;
  }

  @Test
  public void testClose()
  {
    try (InputStream inputStream = new AsyncQueryResettingFilterInputStream(
        new FileInputStream(file),
        testRunnable,
        ASYNC_LAST_UPDATE_TRIGGER_TIME,
        clock
    )) {
      Assert.assertEquals(0, testRunnable.timesCalled());
    }
    catch (IOException e) {
      Assert.fail(String.format(Locale.US, "shouldn't have had an exception %s", e));
    }
    finally {
      Assert.assertEquals(1, testRunnable.timesCalled());
    }
  }

  @Test
  public void testReadNoParams()
  {
    try (InputStream inputStream = new AsyncQueryResettingFilterInputStream(
        new FileInputStream(file),
        testRunnable,
        ASYNC_LAST_UPDATE_TRIGGER_TIME,
        clock
    )) {
      Assert.assertEquals(0, testRunnable.timesCalled());
      inputStream.read();
      Assert.assertEquals(0, testRunnable.timesCalled());
      Mockito.when(clock.millis()).thenReturn(currentTime += ASYNC_LAST_UPDATE_TRIGGER_TIME + 10);
      inputStream.read();
      Assert.assertEquals(1, testRunnable.timesCalled());
      inputStream.read();
      Assert.assertEquals(1, testRunnable.timesCalled());
      Mockito.when(clock.millis()).thenReturn(currentTime += ASYNC_LAST_UPDATE_TRIGGER_TIME);
      inputStream.read();
      Assert.assertEquals(2, testRunnable.timesCalled());
    }
    catch (IOException e) {
      Assert.fail(String.format(Locale.US, "shouldn't have had an exception %s", e));
    }
  }

  @Test
  public void testReadWithByteArrayParam()
  {
    try (InputStream inputStream = new AsyncQueryResettingFilterInputStream(
        new FileInputStream(file),
        testRunnable,
        ASYNC_LAST_UPDATE_TRIGGER_TIME,
        clock
    )) {
      byte[] bytes = new byte[2];
      Assert.assertEquals(0, testRunnable.timesCalled());
      inputStream.read(bytes);
      Assert.assertEquals(0, testRunnable.timesCalled());
      Mockito.when(clock.millis()).thenReturn(currentTime += ASYNC_LAST_UPDATE_TRIGGER_TIME);
      inputStream.read(bytes);
      Assert.assertEquals(1, testRunnable.timesCalled());
      inputStream.read(bytes);
      Assert.assertEquals(1, testRunnable.timesCalled());
      Mockito.when(clock.millis()).thenReturn(currentTime += ASYNC_LAST_UPDATE_TRIGGER_TIME);
      inputStream.read(bytes);
      Assert.assertEquals(2, testRunnable.timesCalled());
    }
    catch (IOException e) {
      Assert.fail(String.format(Locale.US, "shouldn't have had an exception %s", e));
    }
  }

  @Test
  public void testReadWithByteArrayOffsetAndLenParams()
  {
    try (InputStream inputStream = new AsyncQueryResettingFilterInputStream(
        new FileInputStream(file),
        testRunnable,
        ASYNC_LAST_UPDATE_TRIGGER_TIME,
        clock
    )) {
      byte[] bytes = new byte[2];
      Assert.assertEquals(0, testRunnable.timesCalled());
      inputStream.read(bytes, 0, 2);
      Assert.assertEquals(0, testRunnable.timesCalled());
      Mockito.when(clock.millis()).thenReturn(currentTime += ASYNC_LAST_UPDATE_TRIGGER_TIME);
      inputStream.read(bytes, 0, 2);
      Assert.assertEquals(1, testRunnable.timesCalled());
      inputStream.read(bytes, 0, 2);
      Assert.assertEquals(1, testRunnable.timesCalled());
      Mockito.when(clock.millis()).thenReturn(currentTime += ASYNC_LAST_UPDATE_TRIGGER_TIME);
      inputStream.read(bytes, 0, 2);
      Assert.assertEquals(2, testRunnable.timesCalled());
    }
    catch (IOException e) {
      Assert.fail(String.format(Locale.US, "shouldn't have had an exception %s", e));
    }
  }

  private static class TestRunnable implements Runnable
  {
    private int timesCalled;

    TestRunnable()
    {
      this.timesCalled = 0;
    }

    public int timesCalled()
    {
      return timesCalled;
    }

    @Override
    public void run()
    {
      timesCalled++;
    }
  }
}
