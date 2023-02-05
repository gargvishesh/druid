package io.imply.druid.sql.calcite.schema.tables.mapping;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import io.imply.druid.sql.calcite.schema.ImplyExternalDruidSchemaCommonConfig;
import org.apache.druid.java.util.RetryableException;
import org.apache.druid.java.util.common.IAE;
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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

public class ExternalTableFunctionApiMapperImplTest
{
  @Mock
  private HttpClient mockHttpClient;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private ExternalTableFunctionApiMapperImpl externalTable;

  private Request mockRequest;
  private InputStreamFullResponseHolder mockResponseHolder;
  private ListenableFuture mockFuture;

  @Before
  public void setUp()
  {
    MockitoAnnotations.openMocks(this);
    externalTable = Mockito.spy(new ExternalTableFunctionApiMapperImpl(
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
  public void testResponseOk_doRequest_returnsSuccessfully() throws ExecutionException, InterruptedException, RetryableException, IOException
  {
    Mockito.when(mockResponseHolder.getStatus()).thenReturn(HttpResponseStatus.OK);
    Mockito.when(mockFuture.get()).thenReturn(mockResponseHolder);
    Mockito.when(mockHttpClient.go(ArgumentMatchers.any(Request.class),
                                   ArgumentMatchers.any(InputStreamFullResponseHandler.class),
                                   ArgumentMatchers.any())).thenReturn(mockFuture);

    InputStreamFullResponseHolder responseHolder1 = externalTable.doRequest(mockRequest);
    Assert.assertEquals(mockResponseHolder, responseHolder1);
  }

  @Test
  public void testResponse404_doRequest_throwsIAEException() throws ExecutionException, InterruptedException
  {
    Mockito.when(mockResponseHolder.getStatus()).thenReturn(HttpResponseStatus.BAD_REQUEST);
    Mockito.when(mockResponseHolder.getContent()).thenReturn(new ByteArrayInputStream(
        StringUtils.format("some exception occurred").getBytes(StandardCharsets.UTF_8)));
    Mockito.when(mockFuture.get()).thenReturn(mockResponseHolder);
    Mockito.when(mockHttpClient.go(ArgumentMatchers.any(Request.class),
                                   ArgumentMatchers.any(InputStreamFullResponseHandler.class),
                                   ArgumentMatchers.any())).thenReturn(mockFuture);


    IAE ex = Assert.assertThrows(
        IAE.class,
        () -> externalTable.doRequest(mockRequest)
    );
    Assert.assertTrue(ex.getMessage().contains("POLARIS_RETURNED_EXCEPTION"));
  }


  @Test
  public void testResponse5xx_doRequest_throwsRetryableException() throws ExecutionException, InterruptedException
  {
    Mockito.when(mockResponseHolder.getStatus()).thenReturn(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    Mockito.when(mockResponseHolder.getContent()).thenReturn(new ByteArrayInputStream(
        StringUtils.format("some exception occurred").getBytes(StandardCharsets.UTF_8)));
    Mockito.when(mockFuture.get()).thenReturn(mockResponseHolder);
    Mockito.when(mockHttpClient.go(ArgumentMatchers.any(Request.class),
                                   ArgumentMatchers.any(InputStreamFullResponseHandler.class),
                                   ArgumentMatchers.any())).thenReturn(mockFuture);

    RetryableException ex = Assert.assertThrows(
        RetryableException.class,
        () -> externalTable.doRequest(mockRequest)
    );
    Assert.assertFalse(ex.getMessage().contains("POLARIS_RETURNED_EXCEPTION"));
  }
}
