package io.imply.druid.license;

import com.google.inject.Binder;
import com.google.inject.Inject;
import org.apache.druid.initialization.DruidModule;

import java.util.Properties;

public class TestImplyLicenseManagerModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(ImplyLicenseManagerProvider.class).to(TestImplyLicenseManagerProvider.class);
  }

  public static class TestImplyLicenseManagerProvider extends ImplyLicenseManagerProvider
  {
    private final ImplyLicenseManager licenseManager = new TestingImplyLicenseManager(null);

    @Inject
    public TestImplyLicenseManagerProvider(Properties properties)
    {
      super(properties);
    }

    @Override
    public ImplyLicenseManager get()
    {
      return licenseManager;
    }
  }
}
