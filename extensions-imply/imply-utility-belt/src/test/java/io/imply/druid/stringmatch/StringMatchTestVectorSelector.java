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

package io.imply.druid.stringmatch;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.util.List;

public class StringMatchTestVectorSelector implements SingleValueDimensionVectorSelector
{
  private static final int MAX_VECTOR_SIZE = 100;

  private final DimensionDictionarySelector dictionarySelector;
  private final int[] currentVector = new int[MAX_VECTOR_SIZE];
  private int currentVectorSize;

  public StringMatchTestVectorSelector(List<String> dictionary)
  {
    this.dictionarySelector = new StringMatchTestDimensionSelector(dictionary);
  }

  public void setCurrentVector(final List<String> strings)
  {
    if (strings.size() > MAX_VECTOR_SIZE) {
      throw new ISE("No");
    }

    currentVectorSize = strings.size();

    for (int i = 0; i < currentVectorSize; i++) {
      String string = strings.get(i);
      currentVector[i] = idLookup().lookupId(string);
      if (currentVector[i] < 0) {
        throw new ISE("No such string[%s]", string);
      }
    }
  }

  @Override
  public int getValueCardinality()
  {
    return dictionarySelector.getValueCardinality();
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    return dictionarySelector.lookupName(id);
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return dictionarySelector.nameLookupPossibleInAdvance();
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return dictionarySelector.idLookup();
  }

  @Override
  public int[] getRowVector()
  {
    return currentVector;
  }

  @Override
  public int getMaxVectorSize()
  {
    return MAX_VECTOR_SIZE;
  }

  @Override
  public int getCurrentVectorSize()
  {
    return currentVector.length;
  }
}
