/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.autoscaling;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.imply.druid.autoscaling.client.ImplyManagerServiceClient;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.indexing.overlord.autoscaling.AutoScalingData;
import org.apache.druid.indexing.overlord.autoscaling.SimpleWorkerProvisioningConfig;
import org.apache.druid.indexing.overlord.setup.AffinityConfig;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.FillCapacityWithAffinityWorkerSelectStrategy;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class ImplyManagerAutoScalerTest
{
  private static final String WORKER_VERSION = "678";
  private static final int MIN_NUM_WORKER = 2;
  private static final int MAX_NUM_WORKER = 5;
  private static final String IP_1 = "1.2.3.4";
  private static final String ID_1 = "id1";
  private static final String IP_2 = "5.6.7.8";
  private static final String ID_2 = "id2";
  private static final String IP_3 = "11.12.13.14";
  private static final String ID_3 = "id3";

  @Mock
  private ImplyManagerServiceClient mockImplyManagerServiceClient;

  @Mock
  private SimpleWorkerProvisioningConfig mockConfig;

  @Mock
  private ImplyManagerEnvironmentConfig mockImplyManagerEnvironmentConfig;

  private ImplyManagerAutoScaler implyManagerAutoScaler;

  @Before
  public void setUp()
  {
    implyManagerAutoScaler = new ImplyManagerAutoScaler(
        MIN_NUM_WORKER,
        MAX_NUM_WORKER,
        mockImplyManagerEnvironmentConfig,
        mockImplyManagerServiceClient,
        mockConfig
    );
    Mockito.when(mockConfig.getWorkerVersion()).thenReturn(WORKER_VERSION);
  }

  @Test
  public void testSerde() throws Exception
  {
    DefaultWorkerBehaviorConfig config = new DefaultWorkerBehaviorConfig(
        new FillCapacityWithAffinityWorkerSelectStrategy(
            new AffinityConfig(
                ImmutableMap.of("foo", ImmutableSet.of("localhost")),
                false
            )
        ),
        new ImplyManagerAutoScaler(
            7,
            11,
            new ImplyManagerEnvironmentConfig(
                "implymanager.io",
                "cluster-id-123"
            ),
            null,
            null
        )
    );

    final ObjectMapper mapper = new DefaultObjectMapper();
    mapper.registerSubtypes(ImplyManagerAutoScaler.class);
    mapper.setInjectableValues(
        new InjectableValues()
        {
          @Override
          public Object findInjectableValue(
              Object valueId,
              DeserializationContext ctxt,
              BeanProperty forProperty,
              Object beanInstance
          )
          {
            return null;
          }
        }
    );
    Assert.assertEquals(config, mapper.readValue(mapper.writeValueAsBytes(config), DefaultWorkerBehaviorConfig.class));
  }

  @Test
  public void testConfigEquals()
  {
    EqualsVerifier.forClass(ImplyManagerEnvironmentConfig.class).withNonnullFields(
        "implyAddress", "clusterId"
    ).usingGetClass().verify();
  }

  @Test
  public void testIpToId() throws Exception
  {
    // empty IPs
    List<String> ips1 = Collections.emptyList();
    List<String> ids1 = implyManagerAutoScaler.ipToIdLookup(ips1);
    Assert.assertEquals(0, ids1.size());

    // actually IPs
    List<Instance> mockResponse = ImmutableList.of(
        new Instance("RUNNING", IP_1, ID_1),
        new Instance("RUNNING", IP_2, ID_2)
    );
    Mockito.when(mockImplyManagerServiceClient.listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig))).thenReturn(mockResponse);

    List<String> ids3 = implyManagerAutoScaler.ipToIdLookup(ImmutableList.of(IP_1));
    Assert.assertEquals(1, ids3.size());
    Assert.assertEquals(ID_1, ids3.get(0));

    Mockito.verify(mockImplyManagerServiceClient).listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig));
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);
  }

  @Test
  public void testIdToIp() throws Exception
  {
    // empty IDs
    List<String> ids1 = Collections.emptyList();
    List<String> ips1 = implyManagerAutoScaler.idToIpLookup(ids1);
    Assert.assertEquals(0, ips1.size());

    // actually IDs
    List<Instance> mockResponse = ImmutableList.of(
        new Instance("RUNNING", IP_1, ID_1),
        new Instance("RUNNING", IP_2, ID_2)
    );
    Mockito.when(mockImplyManagerServiceClient.listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig))).thenReturn(mockResponse);

    List<String> ids3 = implyManagerAutoScaler.idToIpLookup(ImmutableList.of(ID_1));
    Assert.assertEquals(1, ids3.size());
    Assert.assertEquals(IP_1, ids3.get(0));

    Mockito.verify(mockImplyManagerServiceClient).listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig));
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);
  }

  @Test
  public void testTerminateWithIdsEmpty()
  {
    // empty Ids
    List<String> ids1 = Collections.emptyList();
    implyManagerAutoScaler.terminateWithIds(ids1);
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);
  }

  @Test
  public void testTerminateWithIdsNonEmpty() throws Exception
  {
    // non-empty ids
    implyManagerAutoScaler.terminateWithIds(ImmutableList.of(ID_1, ID_2));

    // verify
    ArgumentCaptor<String> idsCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockImplyManagerServiceClient, Mockito.times(2)).terminateInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig), idsCaptor.capture());
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);

    final List<String> captured = idsCaptor.getAllValues();
    Assert.assertEquals(2, captured.size());
    Assert.assertTrue(captured.contains(ID_1));
    Assert.assertTrue(captured.contains(ID_2));
  }

  @Test
  public void testTerminateWithEmpty()
  {
    // empty Ips
    List<String> ips1 = Collections.emptyList();
    implyManagerAutoScaler.terminate(ips1);
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);
  }

  @Test
  public void testTerminateWithNonEmpty() throws Exception
  {
    List<Instance> mockResponse = ImmutableList.of(
        new Instance("RUNNING", IP_1, ID_1),
        new Instance("RUNNING", IP_2, ID_2),
        new Instance("TERMINATING", IP_3, ID_3)
    );
    Mockito.when(mockImplyManagerServiceClient.listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig))).thenReturn(mockResponse);

    // non-empty ips
    implyManagerAutoScaler.terminate(ImmutableList.of(IP_1, IP_2));

    // Verify the ip --> id conversion part
    Mockito.verify(mockImplyManagerServiceClient).listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig));

    // verify the terminate part
    ArgumentCaptor<String> idsCaptor = ArgumentCaptor.forClass(String.class);
    Mockito.verify(mockImplyManagerServiceClient, Mockito.times(2)).terminateInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig), idsCaptor.capture());
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);

    final List<String> captured = idsCaptor.getAllValues();
    Assert.assertEquals(2, captured.size());
    Assert.assertTrue(captured.contains(ID_1));
    Assert.assertTrue(captured.contains(ID_2));
  }

  @Test
  public void testProvisionExceedMaximum() throws Exception
  {
    List<Instance> mockResponse = new ArrayList<>();
    for (int i = 0; i < MAX_NUM_WORKER; i++) {
      mockResponse.add(new Instance("RUNNING", "id_999" + i, "1.1.1." + i));
    }
    Mockito.when(mockImplyManagerServiceClient.listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig))).thenReturn(mockResponse);

    AutoScalingData actual = implyManagerAutoScaler.provision();
    Assert.assertNotNull(actual);
    Assert.assertEquals(0, actual.getNodeIds().size());

    Mockito.verify(mockImplyManagerServiceClient).listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig));
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);
  }

  @Test
  public void testProvisionUnderMaximum() throws Exception
  {
    List<Instance> mockResponse = new ArrayList<>();
    for (int i = 0; i < MAX_NUM_WORKER - 1; i++) {
      mockResponse.add(new Instance("RUNNING", "id_999" + i, "1.1.1." + i));
    }
    Mockito.when(mockImplyManagerServiceClient.listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig))).thenReturn(mockResponse);
    Mockito.when(mockImplyManagerServiceClient.provisionInstances(ArgumentMatchers.any(ImplyManagerEnvironmentConfig.class), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt())).thenReturn(ImmutableList.of(ID_3));

    AutoScalingData actual = implyManagerAutoScaler.provision();
    Assert.assertNotNull(actual);
    Assert.assertEquals(1, actual.getNodeIds().size());
    Assert.assertEquals(ID_3, actual.getNodeIds().get(0));

    Mockito.verify(mockImplyManagerServiceClient).listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig));
    Mockito.verify(mockImplyManagerServiceClient).provisionInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig), ArgumentMatchers.eq(WORKER_VERSION), ArgumentMatchers.eq(1));
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);
  }

  @Test
  public void testProvisionUnderMaximumExcludingTerminatingInstance() throws Exception
  {
    List<Instance> mockResponse = new ArrayList<>();
    for (int i = 0; i < MAX_NUM_WORKER - 1; i++) {
      mockResponse.add(new Instance("RUNNING", "id_999" + i, "1.1.1." + i));
    }
    for (int i = 0; i < MAX_NUM_WORKER; i++) {
      mockResponse.add(new Instance("TERMINATING", "id_888" + i, "2.2.2." + i));
    }
    Mockito.when(mockImplyManagerServiceClient.listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig))).thenReturn(mockResponse);
    Mockito.when(mockImplyManagerServiceClient.provisionInstances(ArgumentMatchers.any(ImplyManagerEnvironmentConfig.class), ArgumentMatchers.anyString(), ArgumentMatchers.anyInt())).thenReturn(ImmutableList.of(ID_3));

    AutoScalingData actual = implyManagerAutoScaler.provision();
    Assert.assertNotNull(actual);
    Assert.assertEquals(1, actual.getNodeIds().size());
    Assert.assertEquals(ID_3, actual.getNodeIds().get(0));

    Mockito.verify(mockImplyManagerServiceClient).listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig));
    Mockito.verify(mockImplyManagerServiceClient).provisionInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig), ArgumentMatchers.eq(WORKER_VERSION), ArgumentMatchers.eq(1));
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);
  }

  @Test
  public void testProvisionMissingWorkerVersion() throws Exception
  {
    List<Instance> mockResponse = new ArrayList<>();
    for (int i = 0; i < MAX_NUM_WORKER - 1; i++) {
      mockResponse.add(new Instance("RUNNING", "id_999" + i, "1.1.1." + i));
    }
    Mockito.when(mockImplyManagerServiceClient.listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig))).thenReturn(mockResponse);
    Mockito.when(mockConfig.getWorkerVersion()).thenReturn(null);

    AutoScalingData actual = implyManagerAutoScaler.provision();
    Assert.assertNotNull(actual);
    Assert.assertEquals(0, actual.getNodeIds().size());

    Mockito.verify(mockImplyManagerServiceClient).listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig));
    Mockito.verifyNoMoreInteractions(mockImplyManagerServiceClient);
  }

  @Test
  public void testGetStartingOrRunningInstances() throws Exception
  {
    List<Instance> mockResponse = ImmutableList.of(
        new Instance("STARTING", IP_1, ID_1),
        new Instance("RUNNING", IP_2, ID_2),
        new Instance("TERMINATING", IP_3, ID_3)
    );
    Mockito.when(mockImplyManagerServiceClient.listInstances(ArgumentMatchers.eq(mockImplyManagerEnvironmentConfig))).thenReturn(mockResponse);

    List<String> actual = implyManagerAutoScaler.getStartingOrRunningInstances();
    Assert.assertNotNull(actual);
    Assert.assertEquals(2, actual.size());
    Assert.assertTrue(actual.contains(ID_1));
    Assert.assertTrue(actual.contains(ID_2));
  }
}
