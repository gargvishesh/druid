/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.license;

import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

public class ImplyLicenseManagerProvider implements Provider<ImplyLicenseManager>
{
  private final String implyLicenseConfigValue;
  private final Logger log = new Logger(ImplyLicenseManagerProvider.class);

  @Inject
  public ImplyLicenseManagerProvider(Properties properties)
  {
    implyLicenseConfigValue = properties.getProperty(ImplyLicenseManager.LICENSE_CONFIG, null);
  }

  @Override
  public ImplyLicenseManager get()
  {
    InputStream licenseInputStream;
    // license found in runtime properties takes precedence over the classpath one
    if (implyLicenseConfigValue != null) {
      log.info("Using Imply License found in runtime configuration.");
      return ImplyLicenseManager.make(implyLicenseConfigValue);
    } else {
      licenseInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(ImplyLicenseManager.LICENSE_FILE_NAME);
      if (licenseInputStream != null) {
        log.info("Using Imply License found in classpath.");
      }
    }
    String license = null;
    if (licenseInputStream != null) {
      try (InputStream inputStream = licenseInputStream) {
        license = IOUtils.toString(Objects.requireNonNull(inputStream), StandardCharsets.UTF_8);
      }
      catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return ImplyLicenseManager.make(license);
  }
}
