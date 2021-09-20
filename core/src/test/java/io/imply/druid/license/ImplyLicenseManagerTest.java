/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.license;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Properties;

import static io.imply.druid.license.ImplyLicenseManager.LICENSE_CONFIG;
import static io.imply.druid.license.ImplyLicenseManager.LICENSE_FILE_NAME;

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
  public void testFileLicense()
  {
    ImplyLicenseManager implyLicenseManager = new ImplyLicenseManagerProvider(new Properties()).get();
    Assert.assertTrue(implyLicenseManager.isFeatureEnabled("alerts"));
    Assert.assertFalse(implyLicenseManager.isFeatureEnabled("myCustomFeature"));
  }

  @Test
  public void testConfigLicense()
  {
    NoLicenseClassLoader noLicenseClassLoader = new NoLicenseClassLoader();
    ClassLoader old = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(noLicenseClassLoader);

      Properties properties = new Properties();
      properties.setProperty(LICENSE_CONFIG, "{\"name\":\"home\",\"expiryDate\":\"2032-03-30\",\"features\":[\"ad-tech-aggregators\",\"alerts\",\"cross-tab\"]}|2021-09-12|oZB0hj9t2BqksLNOSp4cymfW5NZpwQSb1v0UicGOCoShhW1ylWtrw8SPdX/plVFxKBWRIIhDQL5w0oSU5Wy7pTpp25lHyKDTx5/sFGOyxKnkdTTPaXDfs8RtSy2MBCmy9s+kLR3zBLhiGT5Ubh0WBsnu7STVGbuX/LztEL793lN5oHlCXm7Nps8xj/B/1x1WvZ9oI/VxWI3SaHdeaNQa4Hwrs6Zf4Y/xUD3Z/v22hZvL9sCYqabjLzeRINMOpU/VH/llS5p8Nsd0oNisHZPLC4mAVfrN/wVllwXVD2YomUQyuXdnjCK2swmf0/m9oLUbZbIpCZjYpdxx313QvSlDEQ==");
      ImplyLicenseManager implyLicenseManager = new ImplyLicenseManagerProvider(properties).get();
      Assert.assertTrue(implyLicenseManager.isFeatureEnabled("alerts"));
      Assert.assertTrue(implyLicenseManager.isFeatureEnabled("ad-tech-aggregators"));
      Assert.assertFalse(implyLicenseManager.isFeatureEnabled("myCustomFeature"));
    }
    finally {
      Thread.currentThread().setContextClassLoader(old);
    }
  }

  @Test
  public void testMultipleLicense()
  {
    Properties properties = new Properties();
    properties.setProperty(LICENSE_CONFIG, "{\"name\":\"home\",\"expiryDate\":\"2032-03-30\",\"features\":[\"ad-tech-aggregators\",\"alerts\",\"cross-tab\"]}|2021-09-12|oZB0hj9t2BqksLNOSp4cymfW5NZpwQSb1v0UicGOCoShhW1ylWtrw8SPdX/plVFxKBWRIIhDQL5w0oSU5Wy7pTpp25lHyKDTx5/sFGOyxKnkdTTPaXDfs8RtSy2MBCmy9s+kLR3zBLhiGT5Ubh0WBsnu7STVGbuX/LztEL793lN5oHlCXm7Nps8xj/B/1x1WvZ9oI/VxWI3SaHdeaNQa4Hwrs6Zf4Y/xUD3Z/v22hZvL9sCYqabjLzeRINMOpU/VH/llS5p8Nsd0oNisHZPLC4mAVfrN/wVllwXVD2YomUQyuXdnjCK2swmf0/m9oLUbZbIpCZjYpdxx313QvSlDEQ==");
    ImplyLicenseManager implyLicenseManager = new ImplyLicenseManagerProvider(properties).get();
    Assert.assertTrue(implyLicenseManager.isFeatureEnabled("alerts"));
    Assert.assertTrue(implyLicenseManager.isFeatureEnabled("ad-tech-aggregators"));
    Assert.assertFalse(implyLicenseManager.isFeatureEnabled("myCustomFeature"));
  }

  @Test
  public void testNoLicense()
  {
    NoLicenseClassLoader noLicenseClassLoader = new NoLicenseClassLoader();
    ClassLoader old = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(noLicenseClassLoader);

      ImplyLicenseManager implyLicenseManager = new ImplyLicenseManagerProvider(new Properties()).get();
      Assert.assertFalse(implyLicenseManager.isFeatureEnabled("alerts"));
      Assert.assertFalse(implyLicenseManager.isFeatureEnabled("myCustomFeature"));
    }
    finally {
      Thread.currentThread().setContextClassLoader(old);
    }
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
    Assert.assertFalse(licenseMetadata.isFeatureEnabled("alerts"));
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

  private static class NoLicenseClassLoader extends URLClassLoader
  {
    public NoLicenseClassLoader()
    {
      super(new URL[]{}, ImplyLicenseManagerTest.class.getClassLoader());
    }

    @Override
    public URL getResource(String name)
    {
      if (name != null && name.startsWith(LICENSE_FILE_NAME)) {
        return null;
      }
      return super.getResource(name);
    }
  }
}
