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
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class StringMatchBufferAggregator implements BufferAggregator
{
  private final DimensionSelector selector;
  private final int maxLength;

  public StringMatchBufferAggregator(DimensionSelector selector, int maxLength)
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
    if (!StringMatchUtil.isMatching(buf, position)) {
      return;
    }

    final IndexedInts row = selector.getRow();
    final int sz = row.size();

    if (sz > 1) {
      StringMatchUtil.putMatching(buf, position, false);
    } else if (sz == 1) {
      final String s = selector.lookupName(row.get(0));
      if (s != null) {
        final String acc = StringMatchUtil.getStringDirect(buf, position);
        if (acc == null) {
          StringMatchUtil.putStringDirect(buf, position, s, maxLength);
        } else if (!acc.equals(StringUtils.chop(s, maxLength))) {
          StringMatchUtil.putMatching(buf, position, false);
        }
      }
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