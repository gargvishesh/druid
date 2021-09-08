/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package org.apache.druid.license;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.io.InputStream;
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
import java.util.Objects;
import java.util.regex.Pattern;

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
  private static final Logger log = new Logger(ImplyLicenseManager.class);
  private static final Integer LICENSE_GRACE_PERIOD_DAYS = 90;
  private static final String INVALID_LICENSE_MESSAGE = "Invalid Imply License";
  private static final String EXPIRED_LICENSE_MESSAGE = "Expired Imply License";
  private final LicenseMetadata licenseMetadata;

  public static ImplyLicenseManager make()
  {
    InputStream licenseInputStream = ImplyLicenseManager.class.getClassLoader().getResourceAsStream("implyLicense.bin");
    try {
      if (licenseInputStream != null) {
        return new ImplyLicenseManager(fetchLicenseMetadata(loadLicense(licenseInputStream)));
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
      throw new IAE(EXPIRED_LICENSE_MESSAGE);
    } else {
      log.info("Validated License : " + jsonPayload);
    }
    return licenseMetadata;
  }

  private static String loadLicense(InputStream licenseInputStream) throws IOException
  {
    try (InputStream inputStream = licenseInputStream) {
      return IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);
    }
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

    public boolean isFeatureEnabled(String featureName)
    {
      return features != null && features.contains(featureName);
    }

    public boolean isExpired()
    {
      LocalDate expirationDate;
      try {
        expirationDate = LocalDate.parse(expiryDate);
      }
      catch (Exception e) {
        return true;
      }
      return LocalDate.now(ZoneId.systemDefault()).isAfter(expirationDate.plus(LICENSE_GRACE_PERIOD_DAYS, ChronoUnit.DAYS));
    }
  }
}
