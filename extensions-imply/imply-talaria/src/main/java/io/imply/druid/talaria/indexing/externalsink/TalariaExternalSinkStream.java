/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.indexing.externalsink;

import java.io.OutputStream;
import java.net.URI;

public class TalariaExternalSinkStream
{
  private final URI uri;
  private final OutputStream outputStream;

  TalariaExternalSinkStream(URI uri, OutputStream outputStream)
  {
    this.uri = uri;
    this.outputStream = outputStream;
  }

  public URI getUri()
  {
    return uri;
  }

  public OutputStream getOutputStream()
  {
    return outputStream;
  }
}
