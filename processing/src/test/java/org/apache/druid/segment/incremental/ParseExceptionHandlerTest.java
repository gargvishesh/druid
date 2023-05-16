/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.segment.incremental;

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.EventMap;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.testing.junit.LoggerCaptureRule;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ParseExceptionHandlerTest
{
  @Mock
  EmittingLogger emittingLogger;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Rule
  public LoggerCaptureRule logger = new LoggerCaptureRule(ParseExceptionHandler.class);

  private static final String POLARIS_SYSTEM_PROPERTY_KEY = "IS_POLARIS";

  @Before
  public void cleanUpBefore()
  {
    clearSystemProperty();
  }

  @After
  public void cleanUpAfter()
  {
    clearSystemProperty();
  }

  public static void clearSystemProperty()
  {
    System.clearProperty(POLARIS_SYSTEM_PROPERTY_KEY);
  }

  @Test
  public void testMetricWhenAllConfigurationsAreTurnedOff()
  {
    final ParseException parseException = new ParseException(null, "test");
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        0
    );

    IntStream.range(0, 100).forEach(i -> {
      parseExceptionHandler.handle(parseException);
      Assert.assertEquals(i + 1, rowIngestionMeters.getUnparseable());
    });
  }

  @Test
  public void testLogParseExceptions()
  {
    final ParseException parseException = new ParseException(null, "test");
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        true,
        Integer.MAX_VALUE,
        0
    );
    parseExceptionHandler.handle(parseException);

    List<LogEvent> logEvents = logger.getLogEvents();
    Assert.assertEquals(1, logEvents.size());
    String logMessage = logEvents.get(0).getMessage().getFormattedMessage();
    Assert.assertTrue(logMessage.contains("Encountered parse exception"));
  }

  @Test
  public void testGetSavedParseExceptionsReturnNullWhenMaxSavedParseExceptionsIsZero()
  {
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        new SimpleRowIngestionMeters(),
        false,
        Integer.MAX_VALUE,
        0
    );
    Assert.assertNull(parseExceptionHandler.getSavedParseExceptionReports());
  }

  @Test
  public void testMaxAllowedParseExceptionsThrowExceptionWhenItHitsMax()
  {
    final ParseException parseException = new ParseException(null, "test");
    final int maxAllowedParseExceptions = 3;
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        maxAllowedParseExceptions,
        0
    );

    IntStream.range(0, maxAllowedParseExceptions).forEach(i -> parseExceptionHandler.handle(parseException));
    Assert.assertEquals(3, rowIngestionMeters.getUnparseable());

    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Max parse exceptions[3] exceeded");
    try {
      parseExceptionHandler.handle(parseException);
    }
    catch (RuntimeException e) {
      Assert.assertEquals(4, rowIngestionMeters.getUnparseable());
      throw e;
    }
  }

  @Test
  public void testGetSavedParseExceptionsReturnMostRecentParseExceptions()
  {
    final int maxSavedParseExceptions = 3;
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        maxSavedParseExceptions,
        emittingLogger
    );
    Assert.assertNotNull(parseExceptionHandler.getSavedParseExceptionReports());
    int exceptionCounter = 0;
    for (; exceptionCounter < maxSavedParseExceptions; exceptionCounter++) {
      parseExceptionHandler.handle(new ParseException(null, StringUtils.format("test %d", exceptionCounter)));
    }
    Assert.assertEquals(3, rowIngestionMeters.getUnparseable());
    Assert.assertEquals(maxSavedParseExceptions, parseExceptionHandler.getSavedParseExceptionReports().size());
    for (int i = 0; i < maxSavedParseExceptions; i++) {
      Assert.assertEquals(
          StringUtils.format("test %d", i),
          parseExceptionHandler.getSavedParseExceptionReports().get(i).getDetails().get(0)
      );
    }
    for (; exceptionCounter < 5; exceptionCounter++) {
      parseExceptionHandler.handle(new ParseException(null, StringUtils.format("test %d", exceptionCounter)));
    }
    Assert.assertEquals(5, rowIngestionMeters.getUnparseable());

    Assert.assertEquals(maxSavedParseExceptions, parseExceptionHandler.getSavedParseExceptionReports().size());
    for (int i = 0; i < maxSavedParseExceptions; i++) {
      Assert.assertEquals(
          StringUtils.format("test %d", i + 2),
          parseExceptionHandler.getSavedParseExceptionReports().get(i).getDetails().get(0)
      );
    }
  }

  @Test
  public void testEmittingParseExceptionsEmitsAsManyExpected()
  {
    System.setProperty(POLARIS_SYSTEM_PROPERTY_KEY, "true");

    ArgumentCaptor<ServiceEventBuilder> captor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    final int maxSavedParseExceptions = 1;
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        maxSavedParseExceptions,
        emittingLogger
    );
    for (int ii = 0; ii < 3; ii++) {
      parseExceptionHandler.handle(new ParseException("input", StringUtils.format("test %d", ii)));
    }
    verify(emittingLogger, times(1)).emit(captor.capture());
    ServiceEventBuilder eventBuilder = captor.getValue();
    EventMap eventMap = eventBuilder.build(ImmutableMap.of()).toMap();
    Map<String, Object> data = (Map<String, Object>) eventMap.get("data");
    List<String> details = (List<String>) data.get("details");
    Assert.assertEquals(details.get(0), "test 0");
  }

  @Test
  public void testEmittingParseExceptionsNotPolarisDoesntEmitEvent()
  {
    ArgumentCaptor<ServiceEventBuilder> captor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    final int maxSavedParseExceptions = 1;
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final ParseExceptionHandler parseExceptionHandler = new ParseExceptionHandler(
        rowIngestionMeters,
        false,
        Integer.MAX_VALUE,
        maxSavedParseExceptions,
        emittingLogger
    );
    for (int ii = 0; ii < 3; ii++) {
      parseExceptionHandler.handle(new ParseException("input", StringUtils.format("test %d", ii)));
    }
    verify(emittingLogger, times(0)).emit(captor.capture());
  }

  @Test
  public void testParseExceptionReportEquals()
  {
    EqualsVerifier.forClass(ParseExceptionReport.class)
                  .withNonnullFields("errorType", "details", "timeOfExceptionMillis")
                  .usingGetClass()
                  .verify();
  }
}
