/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package org.apache.druid.license;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class ImplyLicenseManagerTest
{
  @Test
  public void testVerifySignature() throws Exception
  {
    String signature = "a9mtENomb6ed1PhyJQcvUzySnWTGyjIuCE0v+WYDCn+Y97O/WIZ"
                 + "/e3fWLDs4ufnZjk186o0CsxnLiPFd1BM5r1FRAgW7bzMfYgEBHnhLazEkzEcRBKYPCIy5GpZMDOxpwons/yckK1wz/XF+gLGz5hP0NQUESsLXA6omQQZYnAxnMTbde0hwCtb81HBEqaVM6T5SUA0x4NGAaWw6qABPYJ/AtyUQei3deHVG4oqooeYNFpmA4n+9fP9i6kLVIsgBBQXbSAHoK9Uq00B4Aa/GKSHCpEsboCGJprLq72mXRc8obL0ktw8eB+Wrs23by+bdpA/XhtvE0zCnFK4ONkW/5g==";
    String message = "{\"name\":\"Imply\",\"expiryDate\":\"2029-01-01\",\"features\":[\"alerts\","
                     + "\"cross-tab\"]}|2019-07-10";
    Assert.assertTrue(ImplyLicenseManager.verifySignature(message, signature, ImplyLicenseManager.PUBLIC_KEY_OLD));
  }

  @Test
  public void testLicense()
  {
    ImplyLicenseManager implyLicenseManager = ImplyLicenseManager.make();
    Assert.assertTrue(implyLicenseManager.isFeatureEnabled("alerts"));
    Assert.assertFalse(implyLicenseManager.isFeatureEnabled("myCustomFeature"));
  }

  @Test
  public void testLicenseMetadata() throws Exception
  {
    String jsonPayload = "{\"customerName\":\"Imply\",\"expiryDate\":\"2029-01-01\",\"features\":[\"alerts\",\"cross-tab\"]}";
    ObjectMapper objectMapper = new ObjectMapper();
    ImplyLicenseManager.LicenseMetadata licenseMetadata = objectMapper.readValue(jsonPayload, ImplyLicenseManager.LicenseMetadata.class);
    Assert.assertFalse(licenseMetadata.isExpired());
  }

  @Test
  public void testExpiredLicenseMetadata() throws Exception
  {
    String jsonPayload = "{\"customerName\":\"Imply\",\"expiryDate\":\"2019-01-01\",\"features\":[\"alerts\",\"cross-tab\"]}";
    ObjectMapper objectMapper = new ObjectMapper();
    ImplyLicenseManager.LicenseMetadata licenseMetadata = objectMapper.readValue(jsonPayload, ImplyLicenseManager.LicenseMetadata.class);
    Assert.assertTrue(licenseMetadata.isExpired());
  }

  @Test
  public void testNoLicenseMetadata() throws Exception
  {
    String jsonPayload = ImplyLicenseManager.NO_LICENSE;
    ObjectMapper objectMapper = new ObjectMapper();
    ImplyLicenseManager.LicenseMetadata licenseMetadata = objectMapper.readValue(jsonPayload, ImplyLicenseManager.LicenseMetadata.class);
    Assert.assertFalse(licenseMetadata.isFeatureEnabled("myCustomFeature"));
    Assert.assertTrue(licenseMetadata.isExpired());
  }
}
