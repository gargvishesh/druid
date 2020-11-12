/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.security;

import org.junit.Test;

public class TokenValidatorTest
{
  @Test
  public void testMultipleThreads() throws Exception
  {
    SymmetricTokenValidator validator = new SymmetricTokenValidator("hello");

    for (int i = 0; i < 20; i++) {
      Runnable runnable = new Runnable()
      {
        @Override
        public void run()
        {
          while (true) {
            validator.validate("afsdsa", "jfojdslfdjlgsd");
          }
        }
      };
      Thread thread = new Thread(runnable);
      thread.start();
    }

    Thread.sleep(5000);
  }
}
