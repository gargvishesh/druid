/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.security;

import org.apache.druid.java.util.common.StringUtils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class SymmetricTokenValidator implements ImplyTokenValidator
{
  private static final Base64.Encoder ENCODER = Base64.getEncoder();

  // Not thread-safe, clone() before using.
  private final Mac hmac;

  public SymmetricTokenValidator(
      String secret
  ) throws NoSuchAlgorithmException, InvalidKeyException
  {
    SecretKeySpec secretKeySpec = new SecretKeySpec(
        StringUtils.toUtf8(secret),
        "HmacSHA256"
    );

    this.hmac = Mac.getInstance("HmacSHA256");
    hmac.init(secretKeySpec);
  }

  @Override
  public boolean validate(String token, String receivedHmac)
  {
    String localHmac = getSignature(token);
    return localHmac.equals(receivedHmac);
  }

  @Override
  public String getSignature(String msg)
  {
    try {
      Mac clonedHmac = (Mac) hmac.clone();
      return ENCODER.encodeToString(
          clonedHmac.doFinal(StringUtils.toUtf8(msg))
      );
    }
    catch (CloneNotSupportedException cnse) {
      throw new RuntimeException(cnse);
    }
  }
}
