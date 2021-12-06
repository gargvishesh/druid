/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.kernel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Strings;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Comparator;
import java.util.Objects;

public class StageId implements Comparable<StageId>
{
  private static final Comparator<StageId> COMPARATOR =
      Comparator.comparing(StageId::getQueryId)
                .thenComparing(StageId::getStageNumber);

  private final String queryId;
  private final int stageNumber;

  public StageId(final String queryId, final int stageNumber)
  {
    if (Strings.isNullOrEmpty(queryId)) {
      throw new IAE("Null or empty queryId");
    }

    if (stageNumber < 0) {
      throw new IAE("Invalid stageNumber [%s]", stageNumber);
    }

    this.queryId = queryId;
    this.stageNumber = stageNumber;
  }

  @JsonCreator
  public static StageId fromString(final String s)
  {
    final int lastUnderscore = s.lastIndexOf('_');

    if (lastUnderscore > 0 && lastUnderscore < s.length() - 1) {
      final Long stageNumber = GuavaUtils.tryParseLong(s.substring(lastUnderscore + 1));

      if (stageNumber != null && stageNumber >= 0 && stageNumber <= Integer.MAX_VALUE) {
        return new StageId(s.substring(0, lastUnderscore), stageNumber.intValue());
      }
    }

    throw new IAE("Not a valid stage id: [%s]", s);
  }

  public String getQueryId()
  {
    return queryId;
  }

  public int getStageNumber()
  {
    return stageNumber;
  }

  @Override
  public int compareTo(StageId that)
  {
    return COMPARATOR.compare(this, that);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StageId stageId = (StageId) o;
    return stageNumber == stageId.stageNumber && Objects.equals(queryId, stageId.queryId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryId, stageNumber);
  }

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.format("%s_%s", queryId, stageNumber);
  }
}
