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
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;

public class StringMatchCombiningAggregator implements Aggregator
{
  private final ColumnValueSelector<?> selector;
  private final int maxLength;

  boolean matching = true;
  private String acc;

  public StringMatchCombiningAggregator(ColumnValueSelector<?> selector, int maxLength)
  {
    this.selector = selector;
    this.maxLength = maxLength;
  }

  @Override
  public void aggregate()
  {
    if (!matching) {
      return;
    }

    final SerializablePairLongString object = (SerializablePairLongString) selector.getObject();
    if (object == null) {
      return;
    }

    if (object.lhs == StringMatchUtil.MATCHING) {
      if (object.rhs != null) {
        if (acc == null) {
          acc = object.rhs;
        } else if (!acc.equals(object.rhs)) {
          matching = false;
          acc = null;
        }
      }
    } else {
      matching = false;
      acc = null;
    }
  }

  @Nullable
  @Override
  public Object get()
  {
    return new SerializablePairLongString(
        (long) (matching ? StringMatchUtil.MATCHING : StringMatchUtil.NOT_MATCHING),
        StringUtils.chop(acc, maxLength)
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
    acc = null;
  }
}
