/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.security;

public class AllowAllTokenValidator implements ImplyTokenValidator
{
  @Override
  public boolean validate(String token, String receivedHmac)
  {
    return true;
  }

  @Override
  public String getSignature(String token)
  {
    return "allowAll";
  }
}
