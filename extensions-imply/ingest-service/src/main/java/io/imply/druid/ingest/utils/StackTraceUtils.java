/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.ingest.utils;

public class StackTraceUtils
{
  public static final String MESSAGE_MALFORMED_TRACE_RETURN = "";
  public static final String MESSAGE_NULL_INPUT_RETURN = null;

  /**
   * @param stackTraceAsString A string obtained by transforming a complete Java Exception stack trace to string
   * @return The message of the original exception (even if empty as well as leading & trailing whitespace),
   * or null if the input is null.
   * This method only looks at the first line (but requires a multi-line input)
   * of the stack trace to determine that it is well formed.
   * It basically looks for a ':' that separates the message (the string after the colon is the message)
   * <p>
   * Note that this method returns empty for malformed stack trace to differentiate from null input.
   */
  public static String extractMessageFromStackTrace(String stackTraceAsString)
  {
    String retVal = MESSAGE_NULL_INPUT_RETURN;
    if (stackTraceAsString != null) {
      retVal = MESSAGE_MALFORMED_TRACE_RETURN;
      String[] lines = stackTraceAsString.split("\n");
      if (lines.length > 1) {
        String firstLine = lines[0];
        int colonLocation = firstLine.indexOf(':');
        if (colonLocation > -1 && colonLocation < firstLine.length()) {
          retVal = firstLine.substring(colonLocation + 1);
        }
      }
    }
    return retVal;
  }


  /**
   * @param messageOrstackTraceAsString If the input is a single line it assumes that is the message
   *                                    and returns it.
   *                                    Otherwise, if the input is a well formed, multiline, java stack trace
   *                                    as a string then
   *                                    this returns the message in the trace.
   * @return The message of the original exception (as well as leading & trailing whitespace),
   * or null if the stack trace is malformed.
   *
   * Use this method if you cannot predict that a message will be either a message or a stack trace.
   */
  public static String extractMessageFromStackTraceIfNeeded(String messageOrstackTraceAsString)
  {
    String retVal = extractMessageFromStackTrace(messageOrstackTraceAsString);
    if (retVal != null) { // malformed
      if (!messageOrstackTraceAsString.contains("\n")) {
        return messageOrstackTraceAsString;
      }
    }
    return retVal;
  }
}
