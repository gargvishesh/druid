/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.error;

import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * An unchecked exception that holds a {@link TalariaFault}.
 */
public class TalariaException extends RuntimeException
{
  private final TalariaFault fault;

  public TalariaException(
      @Nullable final Throwable cause,
      final TalariaFault fault
  )
  {
    super(fault.getCodeWithMessage(), cause);
    this.fault = Preconditions.checkNotNull(fault, "fault");
  }

  public TalariaException(final TalariaFault fault)
  {
    this(null, fault);
  }

  public TalariaFault getFault()
  {
    return fault;
  }
}
