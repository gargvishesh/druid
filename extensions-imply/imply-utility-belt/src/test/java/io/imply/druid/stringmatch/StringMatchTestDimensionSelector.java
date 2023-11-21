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

import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.List;

public class StringMatchTestDimensionSelector implements DimensionSelector, IdLookup
{
  private final List<String> dictionary;
  private final ArrayBasedIndexedInts currentRow = new ArrayBasedIndexedInts();

  public StringMatchTestDimensionSelector(List<String> dictionary)
  {
    this.dictionary = dictionary;
  }

  public void setCurrentRow(List<String> strings)
  {
    currentRow.ensureSize(strings.size());
    currentRow.setSize(strings.size());
    for (int i = 0; i < strings.size(); i++) {
      String string = strings.get(i);
      final int id = lookupId(string);
      currentRow.setValue(i, id);
    }
  }

  @Override
  public int getValueCardinality()
  {
    return dictionary.size();
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    return dictionary.get(id);
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return true;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return this;
  }

  @Override
  public int lookupId(@Nullable String name)
  {
    return dictionary.indexOf(name);
  }

  @Override
  public IndexedInts getRow()
  {
    return currentRow;
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public Object getObject()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Class<?> classOfObject()
  {
    throw new UnsupportedOperationException();
  }
}
