/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.calcite.schema.tables.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.sql.calcite.external.PolarisExternalTableSpec;
import io.imply.druid.sql.calcite.external.PolarisTestTableFunctionUtils;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHandler;
import org.apache.druid.java.util.http.client.response.InputStreamFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class ExternalTableFunctionApiMapperImplTest
{
  @Mock
  private HttpClient mockHttpClient;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private ExternalTableFunctionApiMapperImpl tableFunctionApiMapper;

  private Request mockRequest;
  private InputStreamFullResponseHolder mockResponseHolder;
  private ListenableFuture mockFuture;

  @Before
  public void setUp()
  {
    MockitoAnnotations.openMocks(this);
    tableFunctionApiMapper = Mockito.spy(new ExternalTableFunctionApiMapperImpl(
        new ImplyExternalDruidSchemaCommonConfig(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            "http://test:9000/v2/tableSchemas",
            "http://test:9000/v2/jobsDML/internal/tableFunctionMapping"
        ),
        objectMapper,
        mockHttpClient
    ));

    mockRequest = Mockito.mock(Request.class);
    mockResponseHolder = Mockito.mock(InputStreamFullResponseHolder.class);
    mockFuture = Mockito.mock(ListenableFuture.class);
  }

  @Test
  public void testValid() throws ExecutionException, InterruptedException, JsonProcessingException
  {
    PolarisExternalTableSpec expectedExternalTableSpec = PolarisTestTableFunctionUtils.getSamplePolarisExternalTableSpec();
    Mockito.when(mockResponseHolder.getStatus()).thenReturn(HttpResponseStatus.OK);
    Mockito.when(mockResponseHolder.getContent()).thenReturn(new ByteArrayInputStream(
        StringUtils.format(objectMapper.writeValueAsString(expectedExternalTableSpec))
                   .getBytes(StandardCharsets.UTF_8)));
    Mockito.when(mockFuture.get()).thenReturn(mockResponseHolder);
    Mockito.when(mockHttpClient.go(
        ArgumentMatchers.any(Request.class),
        ArgumentMatchers.any(InputStreamFullResponseHandler.class),
        ArgumentMatchers.any()
    )).thenReturn(mockFuture);

    Assert.assertEquals(expectedExternalTableSpec, tableFunctionApiMapper.getTableFunctionMapping(
        PolarisTestTableFunctionUtils.getSampleTableFunctionSpec()));
  }

  @Test
  public void testResponse4xx_doRequest_throwsIAEException() throws ExecutionException, InterruptedException
  {
    String errorMsg = "User specified file not found. Correct the issue and then retry";
    Mockito.when(mockResponseHolder.getStatus()).thenReturn(HttpResponseStatus.BAD_REQUEST);
    Mockito.when(mockResponseHolder.getContent()).thenReturn(new ByteArrayInputStream(
        StringUtils.format(errorMsg).getBytes(StandardCharsets.UTF_8)));
    Mockito.when(mockFuture.get()).thenReturn(mockResponseHolder);
    Mockito.when(mockHttpClient.go(
        ArgumentMatchers.any(Request.class),
        ArgumentMatchers.any(InputStreamFullResponseHandler.class),
        ArgumentMatchers.any()
    )).thenReturn(mockFuture);


    RuntimeException ex = Assert.assertThrows(
        RuntimeException.class,
        () -> tableFunctionApiMapper.getTableFunctionMapping(PolarisTestTableFunctionUtils.getSampleTableFunctionSpec())
    );

    Assert.assertTrue(ex.getMessage().contains(errorMsg));
    Assert.assertTrue(ex.getMessage().contains(ExternalTableFunctionApiMapperImpl.POLARIS_EXCEPTION_TAG));
  }


  @Test
  public void test_Response5xx_doRequest_throwsRetryableException() throws ExecutionException, InterruptedException
  {
    String errorMsg = "Some internal error occurred on Polaris. Please retry.";
    Mockito.when(tableFunctionApiMapper.getPolarisRetryCount()).thenReturn(1);
    Mockito.when(mockResponseHolder.getStatus()).thenReturn(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    Mockito.when(mockResponseHolder.getContent()).thenReturn(new ByteArrayInputStream(
        StringUtils.format(errorMsg).getBytes(StandardCharsets.UTF_8)));
    Mockito.when(mockFuture.get()).thenReturn(mockResponseHolder);
    Mockito.when(mockHttpClient.go(
        ArgumentMatchers.any(Request.class),
        ArgumentMatchers.any(InputStreamFullResponseHandler.class),
        ArgumentMatchers.any()
    )).thenReturn(mockFuture);

    Exception ex = Assert.assertThrows(
        Exception.class,
        () -> tableFunctionApiMapper.getTableFunctionMapping(PolarisTestTableFunctionUtils.getSampleTableFunctionSpec())
    );
    Assert.assertTrue(ex.getMessage().contains(errorMsg));
    Assert.assertFalse(ex.getMessage().contains(ExternalTableFunctionApiMapperImpl.POLARIS_EXCEPTION_TAG));
  }
}
