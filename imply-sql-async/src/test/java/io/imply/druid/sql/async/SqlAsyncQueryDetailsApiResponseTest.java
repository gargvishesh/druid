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

package io.imply.druid.sql.async;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.imply.druid.sql.async.SqlAsyncQueryDetails.State;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.sql.http.ResultFormat;
import org.junit.Assert;
import org.junit.Test;

public class SqlAsyncQueryDetailsApiResponseTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SqlAsyncQueryDetailsApiResponse.class).usingGetClass().verify();
  }

  @Test
  public void testJsonSerialization() throws JsonProcessingException
  {
    final ObjectWriter objectWriter = mapper.writerWithDefaultPrettyPrinter();
    final SqlAsyncQueryDetailsApiResponse runningResponse = new SqlAsyncQueryDetailsApiResponse(
        "testId",
        State.RUNNING,
        ResultFormat.OBJECT,
        80,
        null
    );
    Assert.assertEquals(
        "{\n"
        + "  \"asyncResultId\" : \"testId\",\n"
        + "  \"state\" : \"RUNNING\",\n"
        + "  \"resultFormat\" : \"object\",\n"
        + "  \"resultLength\" : 80\n"
        + "}",
        objectWriter.writeValueAsString(runningResponse)
    );

    final SqlAsyncQueryDetailsApiResponse failedResponse = new SqlAsyncQueryDetailsApiResponse(
        "testId",
        State.FAILED,
        ResultFormat.OBJECT,
        0,
        new QueryTimeoutException(
            QueryTimeoutException.ERROR_CODE,
            QueryTimeoutException.ERROR_MESSAGE,
            QueryTimeoutException.class.getName(),
            "testHost"
        )
    );
    Assert.assertEquals(
        "{\n"
        + "  \"asyncResultId\" : \"testId\",\n"
        + "  \"state\" : \"FAILED\",\n"
        + "  \"resultFormat\" : \"object\",\n"
        + "  \"error\" : {\n"
        + "    \"error\" : \"Query timeout\",\n"
        + "    \"errorMessage\" : \"Query Timed Out!\",\n"
        + "    \"errorClass\" : \"org.apache.druid.query.QueryTimeoutException\",\n"
        + "    \"host\" : \"testHost\"\n"
        + "  }\n"
        + "}",
        objectWriter.writeValueAsString(failedResponse)
    );
  }
}
