/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.security.keycloak;

import org.apache.druid.java.util.common.ISE;

import java.util.concurrent.Callable;

public class TestUtils
{
  public static <T> void retryUntil(
      Callable<T> callable,
      T expectedValue,
      long delayInMillis,
      int retryCount,
      String failureMessage
  )
  {
    try {
      int currentTry = 0;
      while (!callable.call().equals(expectedValue)) {
        if (currentTry > retryCount) {
          throw new ISE("Max number of retries [%d] exceeded. %s.", retryCount, failureMessage);
        }

        Thread.sleep(delayInMillis);

        currentTry++;
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
