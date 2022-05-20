/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.rpc;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;

import javax.annotation.Nullable;

public class RpcServerError
{
  private final StringFullResponseHolder responseHolder;

  public RpcServerError(final StringFullResponseHolder responseHolder)
  {
    this.responseHolder = Preconditions.checkNotNull(responseHolder, "responseHolder");
  }

  public StringFullResponseHolder getResponse()
  {
    return responseHolder;
  }

  @Override
  public String toString()
  {
    // Message is included in Either#valueOrThrow, so ensure it makes sense in that context.
    return StringUtils.format(
        "RpcServerError [%s]; %s",
        responseHolder.getStatus(),
        choppedBodyErrorMessage(responseHolder.getContent())
    );
  }

  static String choppedBodyErrorMessage(@Nullable final String responseContent)
  {
    if (responseContent == null || responseContent.isEmpty()) {
      return "no body";
    } else if (responseContent.length() > 1000) {
      final String choppedMessage = StringUtils.chop(responseContent, 1000);
      return "first 1KB of body: " + choppedMessage;
    } else {
      return "body: " + responseContent;
    }
  }
}
