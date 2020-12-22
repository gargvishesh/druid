/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.utils;

import org.junit.Assert;
import org.junit.Test;

public class StackTraceUtilsTest
{
  @Test
  public void extractMessageFromStackTrace_shouldReturnMessageWhenLeadingAndTrailingWhitespace()
  {
    String stackTrace = "org.apache.druid.java.util.common.RE: Max parse exceptions[1000] exceeded \n"
                        + "\tat org.apache.druid.segment.incremental.ParseExceptionHandler.handle(ParseExceptionHandler.java:86) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.FilteringCloseableInputRowIterator.hasNext(FilteringCloseableInputRowIterator.java:74) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.IndexTask.collectIntervalsAndShardSpecs(IndexTask.java:738) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.IndexTask.createShardSpecsFromInput(IndexTask.java:653) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.IndexTask.determineShardSpecs(IndexTask.java:616) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.IndexTask.runTask(IndexTask.java:470) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.AbstractBatchIndexTask.run(AbstractBatchIndexTask.java:140) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask.runSequential(ParallelIndexSupervisorTask.java:964) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask.runTask(ParallelIndexSupervisorTask.java:445) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.AbstractBatchIndexTask.run(AbstractBatchIndexTask.java:140) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner$SingleTaskBackgroundRunnerCallable.call(SingleTaskBackgroundRunner.java:451) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner$SingleTaskBackgroundRunnerCallable.call(SingleTaskBackgroundRunner.java:423) [classes/:?]\n"
                        + "\tat java.util.concurrent.FutureTask.run(FutureTask.java:264) [?:?]\n"
                        + "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) [?:?]\n"
                        + "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) [?:?]\n"
                        + "\tat java.lang.Thread.run(Thread.java:829) [?:?]";
    String message = StackTraceUtils.extractMessageFromStackTrace(stackTrace);
    Assert.assertEquals(" Max parse exceptions[1000] exceeded ", message);
  }

  @Test
  public void extractMessageFromStackTrace_shouldNotReturnMessageWhenMalformed()
  {
    String notAStackTrace = "This is not a stack trace";
    String message = StackTraceUtils.extractMessageFromStackTrace(notAStackTrace);
    Assert.assertEquals(StackTraceUtils.MESSAGE_MALFORMED_TRACE_RETURN, message);
  }

  @Test
  public void extractMessageFromStackTrace_shouldNotReturnMessageWhenOnlySingleLine()
  {
    String notAStackTrace = "Message: not a stack trace";
    String message = StackTraceUtils.extractMessageFromStackTrace(notAStackTrace);
    Assert.assertEquals(StackTraceUtils.MESSAGE_MALFORMED_TRACE_RETURN, message);
  }

  @Test
  public void extractMessageFromStackTrace_shouldNotReturnEmptyStringWhenThereIsNoMessage()
  {
    String stackTrace = "org.apache.druid.java.util.common.RE:\n"
                        + "\tat org.apache.druid.segment.incremental.ParseExceptionHandler.handle(ParseExceptionHandler.java:86) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.FilteringCloseableInputRowIterator.hasNext(FilteringCloseableInputRowIterator.java:74) ~[classes/:?]\n";
    String message = StackTraceUtils.extractMessageFromStackTrace(stackTrace);
    Assert.assertEquals(StackTraceUtils.MESSAGE_MALFORMED_TRACE_RETURN, message);
  }

  @Test
  public void extractMessageFromStackTrace_shouldReturnMessageWhenLeadingWhitespace()
  {
    String testString = "java.lang.ArithmeticException: / by zero\n"
                        + "\tat com.examples.test.StackTrace.main(StackTrace.java:20)";
    String message = StackTraceUtils.extractMessageFromStackTrace(testString);
    Assert.assertEquals(" / by zero", message);
  }

  @Test
  public void extractMessageFromStackTrace_shouldReturnMessageWhenNoLeadingAndTrailingWhitespace()
  {
    String testString = "java.lang.StringIndexOutOfBoundsException:String index out of range: -993\n"
                        + "        at java.lang.String.substring(String.java:1931)\n"
                        + "        at com.examples.test.StackTrace.main(StackTrace.java:27";
    String message = StackTraceUtils.extractMessageFromStackTrace(testString);
    Assert.assertEquals("String index out of range: -993", message);
  }

  @Test
  public void extractMessageFromStackTrace_shouldWorkWithEmptyString()
  {
    String testString = ""; // qualifies as malformed trace wich returns the empty string
    String message = StackTraceUtils.extractMessageFromStackTrace(testString);
    Assert.assertEquals(StackTraceUtils.MESSAGE_MALFORMED_TRACE_RETURN, message);
  }

  @Test
  public void extractMessageFromStackTrace_shouldWorkWithNullString()
  {
    String testString = null;
    String message = StackTraceUtils.extractMessageFromStackTrace(testString);
    Assert.assertEquals(StackTraceUtils.MESSAGE_NULL_INPUT_RETURN, message);
  }

  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldReturnMessageWhenLeadingAndTrailingWhitespace()
  {
    String stackTrace = "org.apache.druid.java.util.common.RE: Max parse exceptions[1000] exceeded \n"
                        + "\tat org.apache.druid.segment.incremental.ParseExceptionHandler.handle(ParseExceptionHandler.java:86) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.FilteringCloseableInputRowIterator.hasNext(FilteringCloseableInputRowIterator.java:74) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.IndexTask.collectIntervalsAndShardSpecs(IndexTask.java:738) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.IndexTask.createShardSpecsFromInput(IndexTask.java:653) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.IndexTask.determineShardSpecs(IndexTask.java:616) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.IndexTask.runTask(IndexTask.java:470) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.AbstractBatchIndexTask.run(AbstractBatchIndexTask.java:140) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask.runSequential(ParallelIndexSupervisorTask.java:964) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask.runTask(ParallelIndexSupervisorTask.java:445) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.AbstractBatchIndexTask.run(AbstractBatchIndexTask.java:140) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner$SingleTaskBackgroundRunnerCallable.call(SingleTaskBackgroundRunner.java:451) [classes/:?]\n"
                        + "\tat org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner$SingleTaskBackgroundRunnerCallable.call(SingleTaskBackgroundRunner.java:423) [classes/:?]\n"
                        + "\tat java.util.concurrent.FutureTask.run(FutureTask.java:264) [?:?]\n"
                        + "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) [?:?]\n"
                        + "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) [?:?]\n"
                        + "\tat java.lang.Thread.run(Thread.java:829) [?:?]";
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(stackTrace);
    Assert.assertEquals(" Max parse exceptions[1000] exceeded ", message);
  }

  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldReturnMessage()
  {
    String singleMessage = "This is not a stack trace";
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(singleMessage);
    Assert.assertEquals(singleMessage, message);
  }


  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldReturnEmptyStringWhenThereIsEmptyMessage()
  {
    String stackTrace = "org.apache.druid.java.util.common.RE:\n"
                        + "\tat org.apache.druid.segment.incremental.ParseExceptionHandler.handle(ParseExceptionHandler.java:86) ~[classes/:?]\n"
                        + "\tat org.apache.druid.indexing.common.task.FilteringCloseableInputRowIterator.hasNext(FilteringCloseableInputRowIterator.java:74) ~[classes/:?]\n";
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(stackTrace);
    Assert.assertEquals(StackTraceUtils.MESSAGE_MALFORMED_TRACE_RETURN, message);
  }


  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldReturnMessageWhenLeadingWhitespace()
  {
    String testString = "java.lang.ArithmeticException: / by zero\n"
                        + "\tat com.examples.test.StackTrace.main(StackTrace.java:20)";
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(testString);
    Assert.assertEquals(" / by zero", message);
  }

  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldReturnMessageWhenNoLeadingAndTrailingWhitespace()
  {
    String testString = "java.lang.StringIndexOutOfBoundsException:String index out of range: -993\n"
                        + "        at java.lang.String.substring(String.java:1931)\n"
                        + "        at com.examples.test.StackTrace.main(StackTrace.java:27";
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(testString);
    Assert.assertEquals("String index out of range: -993", message);
  }

  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldWorkWithEmptyString()
  {
    String testString = "";
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(testString);
    Assert.assertEquals(StackTraceUtils.MESSAGE_MALFORMED_TRACE_RETURN, message);
  }

  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldWorkWithNullString()
  {
    String testString = null;
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(testString);
    Assert.assertEquals(StackTraceUtils.MESSAGE_NULL_INPUT_RETURN, message);
  }

  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldNotReturnEmptyMessageWhenMultipleLines()
  {
    String singleMessage = "This is not a stack trace\nanother line";
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(singleMessage);
    Assert.assertEquals(StackTraceUtils.MESSAGE_MALFORMED_TRACE_RETURN, message);
  }

  @Test
  public void extractMessageFromStackTraceIfNeeded_shouldReturnMessageWithAnyCharactersButNoNewLines()
  {
    String singleMessage = "This is not a stack trace:another& *\'line";
    String message = StackTraceUtils.extractMessageFromStackTraceIfNeeded(singleMessage);
    Assert.assertEquals(singleMessage, message);
  }

}
