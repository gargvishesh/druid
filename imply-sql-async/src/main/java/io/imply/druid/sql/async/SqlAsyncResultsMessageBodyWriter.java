/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.sql.async;

import com.google.common.io.ByteStreams;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

@Provider
public class SqlAsyncResultsMessageBodyWriter implements MessageBodyWriter<SqlAsyncResults>
{
  @Override
  public boolean isWriteable(
      final Class<?> type,
      final Type genericType,
      final Annotation[] annotations,
      final MediaType mediaType
  )
  {
    return SqlAsyncResults.class.isAssignableFrom(type);
  }

  @Override
  public long getSize(
      final SqlAsyncResults results,
      final Class<?> type,
      final Type genericType,
      final Annotation[] annotations,
      final MediaType mediaType
  )
  {
    return results.getSize() > 0 ? results.getSize() : -1;
  }

  @Override
  public void writeTo(
      final SqlAsyncResults results,
      final Class<?> type,
      final Type genericType,
      final Annotation[] annotations,
      final MediaType mediaType,
      final MultivaluedMap<String, Object> httpHeaders,
      final OutputStream entityStream
  ) throws IOException, WebApplicationException
  {
    try (final InputStream resultStream = results.getInputStream()) {
      ByteStreams.copy(results.getInputStream(), entityStream);
    }
  }
}
