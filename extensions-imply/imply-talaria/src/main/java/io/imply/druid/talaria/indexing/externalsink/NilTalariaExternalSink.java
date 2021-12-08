/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.externalsink;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.ISE;

public class NilTalariaExternalSink implements TalariaExternalSink
{
  public static final String TYPE = "nil";

  @Inject
  public NilTalariaExternalSink()
  {
    // Nothing special to do.
  }

  @Override
  public TalariaExternalSinkStream open(final String queryId, final int partitionNumber)
  {
    throw new ISE("External sinking is not available in your current configuration");
  }
}
