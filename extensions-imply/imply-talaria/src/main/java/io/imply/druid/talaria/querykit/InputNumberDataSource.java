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

@JsonTypeName("inputNumber")
public class InputNumberDataSource implements DataSource
{
  private final int inputNumber;

  @JsonCreator
  public InputNumberDataSource(@JsonProperty("inputNumber") int inputNumber)
  {
    this.inputNumber = inputNumber;
  }

  @Override
  public Set<String> getTableNames()
  {
    // TODO(gianm): there actually may be some tables in the input, but they are getting dropped here
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
  public int getInputNumber()
  {
    return inputNumber;
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
    InputNumberDataSource that = (InputNumberDataSource) o;
    return inputNumber == that.inputNumber;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputNumber);
  }

  @Override
  public String toString()
  {
    return "InputNumberDataSource{" +
           "inputNumber=" + inputNumber +
           '}';
  }
}
