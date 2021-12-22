/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

import org.apache.druid.java.util.http.client.HttpClientConfig;
import org.junit.Assert;
import org.junit.Test;

public class ClarityHttpEmitterModuleTest
{
  @Test
  public void testDefaultWorkerCount()
  {
    HttpClientConfig clientConfig = ClarityHttpEmitterModule.getHttpClientConfig(
        ClarityHttpEmitterConfig.builder("http://metrics.foo.bar/").build(),
        new ClarityHttpEmitterSSLClientConfig()
    );
    Assert.assertEquals(ClarityHttpEmitterModule.getDefaultWorkerCount(), clientConfig.getWorkerPoolSize());
  }

  @Test
  public void testWorkerCount()
  {
    final int workerCount = 12;
    HttpClientConfig clientConfig = ClarityHttpEmitterModule.getHttpClientConfig(
        ClarityHttpEmitterConfig.builder("http://metrics.foo.bar/").withWorkerCount(workerCount).build(),
        new ClarityHttpEmitterSSLClientConfig()
    );
    Assert.assertEquals(workerCount, clientConfig.getWorkerPoolSize());
  }
}
