/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc.
 */

package io.imply.security;

public interface ImplyTokenValidator
{
  boolean validate(String token, String receivedHmac);

  String getSignature(String token);
}
