package org.apache.druid.msq.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.compaction.MSQCompactionProvider;
import org.apache.druid.server.coordinator.duty.CompactionClient;

import java.util.Collections;
import java.util.List;

public class MSQCompactionDruidModule implements DruidModule
{

  public static final String SCHEME = "msq";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(CompactionClient.class).toProvider(MSQCompactionProvider.class).in(LazySingleton.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

}
