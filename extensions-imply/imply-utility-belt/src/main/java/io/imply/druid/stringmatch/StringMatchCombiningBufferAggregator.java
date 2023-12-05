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

import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringMatchCombiningBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<?> selector;
  private final int maxLength;

  public StringMatchCombiningBufferAggregator(ColumnValueSelector<?> selector, int maxLength)
  {
    this.selector = selector;
    this.maxLength = maxLength;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    StringMatchUtil.initDirect(buf, position, maxLength);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final boolean matching = StringMatchUtil.isMatching(buf, position);

    if (!matching) {
      return;
    }

    final SerializablePairLongString object = (SerializablePairLongString) selector.getObject();
    if (object == null) {
      return;
    }

    if (object.lhs == StringMatchUtil.MATCHING) {
      if (object.rhs != null) {
        final String acc = StringMatchUtil.getStringDirect(buf, position);
        if (acc == null) {
          StringMatchUtil.putStringDirect(buf, position, object.rhs, maxLength);
        } else if (!acc.equals(object.rhs)) {
          StringMatchUtil.putMatching(buf, position, false);
        }
      }
    } else {
      StringMatchUtil.putMatching(buf, position, false);
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return StringMatchUtil.getResultDirect(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    return 0;
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    return 0;
  }

  @Override
  public void close()
  {
  }
}
