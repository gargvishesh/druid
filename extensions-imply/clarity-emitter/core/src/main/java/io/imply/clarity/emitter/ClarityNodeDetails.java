/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;

import java.util.Locale;
import java.util.Objects;

/**
 * Everything interesting about a Clarity node.
 */
public class ClarityNodeDetails
{
  private final String nodeType;
  private final String clusterName;
  private final String druidVersion;
  private final String implyVersion;

  public ClarityNodeDetails(
      final String nodeType,
      final String clusterName,
      final String druidVersion,
      final String implyVersion
  )
  {
    this.nodeType = Preconditions.checkNotNull(nodeType, "nodeType");
    this.clusterName = Preconditions.checkNotNull(clusterName, "clusterName");
    this.druidVersion = Preconditions.checkNotNull(druidVersion, "druidVersion");
    this.implyVersion = Preconditions.checkNotNull(implyVersion, "implyVersion");
  }

  public static ClarityNodeDetails fromInjector(final Injector injector, final BaseClarityEmitterConfig config)
  {
    String serviceName;
    try {
      serviceName = injector.getInstance(Key.get(String.class, Names.named("serviceName")));
    }
    catch (Exception e) {
      serviceName = "unknown";
    }

    return new ClarityNodeDetails(
        serviceName,
        //CHECKSTYLE.OFF: Regexp
        Strings.nullToEmpty(config.getClusterName()),
        //CHECKSTYLE.ON: Regexp
        ClarityEmitterUtils.getDruidVersion(),
        ClarityEmitterUtils.getImplyVersion()
    );
  }

  public String getNodeType()
  {
    return nodeType;
  }

  public String getClusterName()
  {
    return clusterName;
  }

  public String getDruidVersion()
  {
    return druidVersion;
  }

  public String getImplyVersion()
  {
    return implyVersion;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ClarityNodeDetails that = (ClarityNodeDetails) o;
    return Objects.equals(nodeType, that.nodeType) &&
           Objects.equals(clusterName, that.clusterName) &&
           Objects.equals(druidVersion, that.druidVersion) &&
           Objects.equals(implyVersion, that.implyVersion);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(nodeType, clusterName, druidVersion, implyVersion);
  }

  @Override
  public String toString()
  {
    return String.format(
        Locale.ENGLISH,
        "clusterName[%s], nodeType[%s], druidVersion[%s], implyVersion[%s]",
        clusterName,
        nodeType,
        druidVersion,
        implyVersion
    );
  }
}
