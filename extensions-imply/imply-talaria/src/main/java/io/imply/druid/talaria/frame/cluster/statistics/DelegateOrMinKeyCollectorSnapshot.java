/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.cluster.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.imply.druid.talaria.frame.cluster.ClusterByKey;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.Objects;

public class DelegateOrMinKeyCollectorSnapshot<T extends KeyCollectorSnapshot> implements KeyCollectorSnapshot
{
  static final String FIELD_SNAPSHOT = "snapshot";
  static final String FIELD_MIN_KEY = "minKey";

  private final T snapshot;
  private final ClusterByKey minKey;

  @JsonCreator
  public DelegateOrMinKeyCollectorSnapshot(
      @JsonProperty(FIELD_SNAPSHOT) final T snapshot,
      @JsonProperty(FIELD_MIN_KEY) final ClusterByKey minKey
  )
  {
    this.snapshot = snapshot;
    this.minKey = minKey;

    if (snapshot != null && minKey != null) {
      throw new ISE("Cannot have both '%s' and '%s'", FIELD_SNAPSHOT, FIELD_MIN_KEY);
    }
  }

  @Nullable
  @JsonProperty(FIELD_SNAPSHOT)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public T getSnapshot()
  {
    return snapshot;
  }

  @Nullable
  @JsonProperty(FIELD_MIN_KEY)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public ClusterByKey getMinKey()
  {
    return minKey;
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
    DelegateOrMinKeyCollectorSnapshot<?> that = (DelegateOrMinKeyCollectorSnapshot<?>) o;
    return Objects.equals(snapshot, that.snapshot) && Objects.equals(minKey, that.minKey);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(snapshot, minKey);
  }
}
