/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.clarity.emitter.http;

public enum BatchingStrategy
{
  ARRAY {
    private final byte[] START = new byte[]{'['};
    private final byte[] SEPARATOR = new byte[]{','};
    private final byte[] END = new byte[]{']', '\n'};

    @Override
    public byte[] batchStart()
    {
      return START;
    }

    @Override
    public byte[] messageSeparator()
    {
      return SEPARATOR;
    }

    @Override
    public byte[] batchEnd()
    {
      return END;
    }
  },
  NEWLINES {
    private final byte[] START = new byte[0];
    private final byte[] SEPARATOR = new byte[]{'\n'};
    private final byte[] END = new byte[]{'\n'};

    @Override
    public byte[] batchStart()
    {
      return START;
    }

    @Override
    public byte[] messageSeparator()
    {
      return SEPARATOR;
    }

    @Override
    public byte[] batchEnd()
    {
      return END;
    }
  };

  public abstract byte[] batchStart();

  public abstract byte[] messageSeparator();

  public abstract byte[] batchEnd();
}
