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
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.IdLookup;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Buffer format:
 *
 * - Direct: 1 byte matching + 4 bytes length (int) + utf-8 string of that length
 * - Dictionary: 1 byte matching + 4 bytes dictionary code
 */
public class StringMatchUtil
{
  /**
   * Argument for {@link #accumulateDictionaryId} "nullId" parameter if the given selector is known to not include null.
   */
  static final int NULL_NOT_PRESENT = -1;

  /**
   * Argument for {@link #accumulateDictionaryId} "nullId" parameter if we don't know whether the given selector
   * includes null.
   */
  static final int NULL_ID_UNKNOWN = -2;

  static final byte MATCHING = 0x01;
  static final byte NOT_MATCHING = 0x02;

  /**
   * Stored as direct string length to signify the string is null.
   */
  static final int NULL_LENGTH = -1;

  /**
   * Stored as dictionary ID to signify there is "nothing" there. Different from null: this is more like undefined.
   */
  static final int NONE_ID = -1;

  private StringMatchUtil()
  {
    // No instantiation.
  }

  static void initDirect(final ByteBuffer buf, final int position, final int maxLength)
  {
    putMatching(buf, position, true);
    putStringDirect(buf, position, null, maxLength);
  }

  static void initDictionary(final ByteBuffer buf, final int position)
  {
    putMatching(buf, position, true);
    putDictionaryId(buf, position, NONE_ID);
  }

  static boolean isMatching(final ByteBuffer buf, final int position)
  {
    return buf.get(position) == MATCHING;
  }

  static void putMatching(final ByteBuffer buf, final int position, final boolean matching)
  {
    buf.put(position, matching ? MATCHING : NOT_MATCHING);
  }

  static void putStringDirect(final ByteBuffer buf, final int position, @Nullable final String s, final int maxLength)
  {
    if (s == null) {
      buf.putInt(position + Byte.BYTES, NULL_LENGTH);
    } else {
      final int originalPosition = buf.position();
      final int originalLimit = buf.limit();
      try {
        buf.position(position + Byte.BYTES + Integer.BYTES);
        buf.limit(buf.position() + maxLength);
        final int len = StringUtils.toUtf8WithLimit(s, buf);
        buf.putInt(position + Byte.BYTES, len);
      }
      finally {
        buf.position(originalPosition);
        buf.limit(originalLimit);
      }
    }
  }

  static int getDictionaryId(final ByteBuffer buf, final int position)
  {
    return buf.getInt(position + Byte.BYTES);
  }

  static void putDictionaryId(final ByteBuffer buf, final int position, final int id)
  {
    buf.putInt(position + Byte.BYTES, id);
  }

  /**
   * Accumulate dictionary id into buffer at a given position. May call
   * {@link DimensionDictionarySelector#lookupName(int)} if nullId is {@link #NULL_ID_UNKNOWN}, but avoids calling
   * that method in all other cases.
   *
   * @param selector selector the dictionary id is from
   * @param buf      buffer
   * @param position position
   * @param id       dictionary id
   * @param nullId   dictionary id of null, or {@link #NULL_NOT_PRESENT}, or {@link #NULL_ID_UNKNOWN}, as appropriate.
   *                 Get this from {@link #getNullId(DimensionDictionarySelector)}
   */
  static void accumulateDictionaryId(
      final DimensionDictionarySelector selector,
      final ByteBuffer buf,
      final int position,
      final int id,
      final int nullId
  )
  {
    if (id == nullId) {
      return;
    }

    final int acc = StringMatchUtil.getDictionaryId(buf, position);
    if (acc == StringMatchUtil.NONE_ID) {
      if (nullId != NULL_ID_UNKNOWN || selector.lookupName(id) != null) {
        StringMatchUtil.putDictionaryId(buf, position, id);
      }
    } else if (acc != id && (nullId != NULL_ID_UNKNOWN || selector.lookupName(id) != null)) {
      StringMatchUtil.putMatching(buf, position, false);
    }
  }

  @Nullable
  static String getStringDirect(final ByteBuffer buf, final int position)
  {
    if (isMatching(buf, position)) {
      final int originalPosition = buf.position();
      try {
        final int length = buf.getInt(position + Byte.BYTES);
        if (length == NULL_LENGTH) {
          return null;
        } else {
          buf.position(position + Byte.BYTES + Integer.BYTES);
          return StringUtils.fromUtf8(buf, length);
        }
      }
      finally {
        buf.position(originalPosition);
      }
    } else {
      return null;
    }
  }

  static SerializablePairLongString getResultDirect(final ByteBuffer buf, final int position)
  {
    if (isMatching(buf, position)) {
      return new SerializablePairLongString((long) MATCHING, getStringDirect(buf, position));
    } else {
      return new SerializablePairLongString((long) NOT_MATCHING, null);
    }
  }

  static SerializablePairLongString getResultWithDictionary(
      final ByteBuffer buf,
      final int position,
      final DimensionDictionarySelector dictionarySelector,
      final int maxLength
  )
  {
    if (isMatching(buf, position)) {
      final int id = buf.getInt(position + Byte.BYTES);
      if (id < 0) {
        return new SerializablePairLongString((long) MATCHING, null);
      } else {
        return new SerializablePairLongString(
            (long) MATCHING,
            StringUtils.chop(dictionarySelector.lookupName(id), maxLength)
        );
      }
    }

    return new SerializablePairLongString((long) NOT_MATCHING, null);
  }

  /**
   * Returns ID of NULL, or {@link StringMatchUtil#NULL_NOT_PRESENT} if NULL is known not to be in the selector, or
   * {@link StringMatchUtil#NULL_ID_UNKNOWN} if we can't tell because there is no {@link IdLookup}.
   */
  static int getNullId(final DimensionDictionarySelector selector)
  {
    final IdLookup idLookup = selector.idLookup();

    if (idLookup != null) {
      final int nullId = idLookup.lookupId(null);
      return nullId >= 0 ? nullId : StringMatchUtil.NULL_NOT_PRESENT;
    } else {
      return StringMatchUtil.NULL_ID_UNKNOWN;
    }
  }
}
