/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.samplinggroupby.hashing;

/**
 * Provides a vectorized interface to compute hashes of values for a column. The startOffset and endOffset represent
 * the range to evaulate in the current vector for the column selector.
 */
public interface HashVectorSupplier
{
  // TODO : should this be a fn from long[] -> long[] for reuse of memory?
  long[] getHashes(int startOffset, int endOffset);
}
