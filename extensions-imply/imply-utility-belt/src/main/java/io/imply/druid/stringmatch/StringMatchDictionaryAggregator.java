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

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

public class StringMatchDictionaryAggregator implements Aggregator
{
  private static final int NONE = -1;

  private final DimensionSelector selector;
  private final int maxLength;
  private final int nullId;

  boolean matching = true;
  private int acc = NONE;

  public StringMatchDictionaryAggregator(DimensionSelector selector, int maxLength)
  {
    this.selector = selector;
    this.maxLength = maxLength;
    this.nullId = StringMatchUtil.getNullId(selector);
  }

  @Override
  public void aggregate()
  {
    if (!matching) {
      return;
    }

    final IndexedInts row = selector.getRow();
    final int sz = row.size();

    if (sz > 1) {
      matching = false;
      acc = NONE;
    } else if (sz == 1) {
      final int id = row.get(0);
      if (id != nullId) {
        if (acc == NONE) {
          acc = id;
        } else if (acc != id) {
          matching = false;
          acc = NONE;
        }
      }
    }
  }

  @Nullable
  @Override
  public Object get()
  {
    return new SerializablePairLongString(
        (long) (matching ? StringMatchUtil.MATCHING : StringMatchUtil.NOT_MATCHING),
        acc == NONE ? null : StringUtils.chop(selector.lookupName(acc), maxLength)
    );
  }

  @Override
  public float getFloat()
  {
    return 0;
  }

  @Override
  public long getLong()
  {
    return 0;
  }

  @Override
  public void close()
  {
    // Nothing to do.
  }
}