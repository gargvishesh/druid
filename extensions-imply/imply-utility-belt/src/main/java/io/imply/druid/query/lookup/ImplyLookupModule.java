package io.imply.druid.query.lookup;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;

import java.util.Collections;
import java.util.List;

/**
 * This class exists to be the place that contains forked changes to druid's {@link org.apache.druid.query.lookup.LookupModule}
 *
 * It exists so that we can hopefully avoid merge conflicts in imports as well as other things by allowing for us to
 * replace singular lines with fully-qualified static calls into this code.
 */
public class ImplyLookupModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    // This binding does not carry a singleton because it is just binding the interface, the provider for the
    // concrete class ImplyLookupProvider carries the singleton annotation instead.
    binder.bind(LookupExtractorFactoryContainerProvider.class).to(ImplyLookupProvider.class);
    binder.bind(ImplyLookupProvider.class).in(LazySingleton.class);
  }

  @Override
  public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("ImplyLookupModule").registerSubtypes(
            new NamedType(SegmentFilteredLookupExtractorFactory.class, "implySegment")
        )
    );
  }
}
