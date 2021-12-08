/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.querykit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.DataSource;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@JsonTypeName("inputStage")
public class InputStageDataSource implements DataSource
{
  private final int stageNumber;

  @JsonCreator
  public InputStageDataSource(@JsonProperty("stageNumber") int stageNumber)
  {
    this.stageNumber = stageNumber;
  }

  @Override
  public Set<String> getTableNames()
  {
    // TODO(gianm): there actually may be some tables in the input stage, but they are getting dropped here
    // TODO(gianm): is this ok? (it probably is, because it's similar to what happens with inline. but should verify.)
    return Collections.emptySet();
  }

  @Override
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(final List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  @Override
  public boolean isConcrete()
  {
    return false;
  }

  @JsonProperty
  public int getStageNumber()
  {
    return stageNumber;
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
    InputStageDataSource that = (InputStageDataSource) o;
    return stageNumber == that.stageNumber;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageNumber);
  }

  @Override
  public String toString()
  {
    return "InputStageDataSource{" +
           "stageNumber=" + stageNumber +
           '}';
  }
}
