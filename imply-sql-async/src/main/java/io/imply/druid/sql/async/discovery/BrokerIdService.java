/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async.discovery;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.discovery.DruidService;

/**
 * A DruidService to announce the "brokerId" along with the broker itself.
 * The brokerId is a unique ID per broker JVM process. If you restart a broker,
 * that broker will have different IDs before and after the restart.
 * This brokerID is currently used by async downloads to identify live brokers.
 *
 * @see io.imply.druid.sql.async.SqlAsyncModule#getBrokerId()
 */
public class BrokerIdService extends DruidService
{
  public static final String NAME = "implyBrokerIdService";

  private final String brokerId;

  @JsonCreator
  public BrokerIdService(@JsonProperty("brokerId") String brokerId)
  {
    this.brokerId = brokerId;
  }

  @JsonProperty
  public String getBrokerId()
  {
    return brokerId;
  }

  @Override
  public String getName()
  {
    return NAME;
  }
}
