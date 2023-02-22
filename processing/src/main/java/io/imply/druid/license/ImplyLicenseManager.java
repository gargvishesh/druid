/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.license;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.KeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;

/**
 * An implementation to protect proprietary features behind an Imply License. The behaviour is as follows :
 * 1. If a license with name {@link #LICENSE_FILE_NAME} is not found in the druid classpath and a config property with
 *    name {@link #LICENSE_CONFIG} is not found, none of the features behind the license are accessible.
 * 2. If a license is found both in classpath as well as config property, the druid process prefers the license found in
 *    config property.
 * 3. If the license is found but is expired, none of the features behind the license are accessible
 * 4. If the license is found and is not verifiable or corrupted, the druid process crashes at startup
 * 5. If the license is found and is valid, the protected features mentioned in the license are accessible.
 *    The validity of a license is marked by a 90 days grace period after the expiration date mentioned in the license.
 */
public class ImplyLicenseManager
{
  // stripped pem files for the public keys
  static final String PUBLIC_KEY = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2SxbNX6vKM9wl9SFnyjL\n"
                                   + "938sIbEnQ09o462rFbB/DQe5T0xvis3P2SjrrZ+sp/spSr4yFGaYFCSXPWo+14TB\n"
                                   + "KOH5+rAII5IlxqR5LRXLVJ836mN6rds8QTm8kumxUO5FVGhGZv8PUB129rfy1s2R\n"
                                   + "fZA/TJfMlIlcMbTzPZgF+2AYDrn54d0I+RVfy7XG3IdtaNGJExxFNwajBEOdh1Xs\n"
                                   + "3kc7nWDBWwIF9vm0GAWg3JxXaPtcT/gHVCDtNvi2AO+ducsHONRWD8JQ7eamzH84\n"
                                   + "hHuSo5qXXppjo0K2lZDJPFd3M/ur1xqd1zrSv9RReqQ+SqrUddNvLARtR0OSAMbe\n"
                                   + "NQIDAQAB\n";
  static final String PUBLIC_KEY_OLD = "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAtUf5vzKS8x4Fi60m8ejD\n"
                                   + "IkzGES3B1YEjqglIbpsa8k1CDsDcoEKzQFP+H5vAcMuWkDDvq+Cw0UiXTHg/RbVo\n"
                                   + "vHtRUzm4ap7x0IO/b4ia+JxRIYokqxKKPpF/JPkHb2qkLh7gfnNmGoRyLe0zCpnZ\n"
                                   + "vIMv51GZ9HV3HzIvcC7SBcBF4QB1BCSooW27Xbs5uKxCeKsNZH0fmTH4FShWkvX/\n"
                                   + "HsE065tk0ptPVs2A0XED0iyFiTlrakRAp7uOHHGZI07nDDAc8TAj7aM0v3lFUh21\n"
                                   + "F8l5c/yrEa58wuI7bXt3sunA+Apji/HTm/Vd2sfUl6sU8224UdNkU04xBaoxWf4J\n"
                                   + "fQIDAQAB\n";
  static final String NO_LICENSE = "{}";
  static final String LICENSE_FILE_NAME = "implyLicense.bin";
  static final String LICENSE_CONFIG = "imply.license";
  private static final Logger log = new Logger(ImplyLicenseManager.class);
  private static final Integer LICENSE_GRACE_PERIOD_DAYS = 90;
  private static final String INVALID_LICENSE_MESSAGE = "Invalid Imply License";
  private final LicenseMetadata licenseMetadata;

  public static ImplyLicenseManager make(String license)
  {
    try {
      if (license != null) {
        return new ImplyLicenseManager(fetchLicenseMetadata(license));
      } else {
        log.info("Unable to find Imply License. Proprietary features may be not accessible");
        return new ImplyLicenseManager(createLicenseMetadata(NO_LICENSE));
      }
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected ImplyLicenseManager(LicenseMetadata licenseMetadata)
  {
    this.licenseMetadata = licenseMetadata;
  }

  /**
   * Checks if a feature is enabled as per the provided license
   * @param featureName
   * @return true if the feature is enabled, else false
   */
  public boolean isFeatureEnabled(String featureName)
  {
    return licenseMetadata.isFeatureEnabled(featureName);
  }

  public static boolean verifySignature(String message, String signature, String publicKey)
      throws NoSuchAlgorithmException, SignatureException, InvalidKeySpecException,
             InvalidKeyException
  {
    Signature verifier = Signature.getInstance("SHA256withRSA");
    String key = Pattern.compile("\\n").matcher(publicKey).replaceAll("");

    KeySpec pubKeySpec = new X509EncodedKeySpec(Base64.getDecoder().decode(key));
    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    verifier.initVerify(keyFactory.generatePublic(pubKeySpec));
    verifier.update(message.getBytes(StandardCharsets.UTF_8));
    return verifier.verify(Base64.getDecoder().decode(signature));
  }

  private static LicenseMetadata fetchLicenseMetadata(String license)
      throws NoSuchAlgorithmException, SignatureException, InvalidKeySpecException, InvalidKeyException,
             JsonProcessingException
  {
    String[] licenseParts = license.trim().split("\\|");
    String jsonPayload;
    String signature;
    if (licenseParts.length == 2) {
      jsonPayload = licenseParts[0];
      signature = licenseParts[1];
      if (!verifySignature(jsonPayload, signature, PUBLIC_KEY)) {
        throw new IAE(INVALID_LICENSE_MESSAGE);
      }
    } else if (licenseParts.length == 3) {
      jsonPayload = licenseParts[0];
      String date = licenseParts[1];
      signature = licenseParts[2];
      if (!verifySignature(String.join("|", jsonPayload, date), signature, PUBLIC_KEY_OLD) ||
          !isValidISODate(date)) {
        throw new IAE(INVALID_LICENSE_MESSAGE);
      }
    } else {
      throw new IAE(INVALID_LICENSE_MESSAGE);
    }
    LicenseMetadata licenseMetadata = createLicenseMetadata(jsonPayload);
    if (licenseMetadata.isExpired()) {
      log.warn("Imply License found expired. Proprietary features may be not accessible");
      return createLicenseMetadata(NO_LICENSE);
    } else {
      log.info("Validated License : " + jsonPayload);
    }
    return licenseMetadata;
  }

  private static LicenseMetadata createLicenseMetadata(String jsonPayload) throws JsonProcessingException
  {
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readValue(jsonPayload, LicenseMetadata.class);
  }

  private static boolean isValidISODate(String date)
  {
    try {
      LocalDate.parse(date);
      return true;
    }
    catch (Exception e) {
      return false;
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  static class LicenseMetadata
  {
    @JsonProperty
    private String expiryDate;
    @JsonProperty
    private List<String> features;
    private volatile Boolean isExpired;

    public boolean isFeatureEnabled(String featureName)
    {
      return !isExpired() && features != null && features.contains(featureName);
    }

    public boolean isExpired()
    {
      if (isExpired == null) {
        if (!isValidISODate(expiryDate)) {
          isExpired = true;
        } else {
          LocalDate expirationDate = LocalDate.parse(expiryDate);
          isExpired = LocalDate.now(ZoneId.systemDefault()).isAfter(expirationDate.plus(LICENSE_GRACE_PERIOD_DAYS, ChronoUnit.DAYS));
        }
      }
      return isExpired;
    }
  }
}
