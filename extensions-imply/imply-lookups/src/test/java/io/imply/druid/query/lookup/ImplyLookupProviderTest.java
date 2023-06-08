/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.lookup;

import com.google.common.collect.Sets;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.query.lookup.LookupIntrospectHandler;
import org.apache.druid.query.lookup.MapLookupExtractorFactory;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

public class ImplyLookupProviderTest
{
  @Test
  public void testSanity()
  {
    final LookupExtractorFactory specializedLookup = new MySpecializableExtractorFactory();
    ArrayList<String> lookupNamesPassed = new ArrayList<>();
    ImplyLookupProvider provider = new ImplyLookupProvider(
        new LookupExtractorFactoryContainerProvider()
        {
          @Override
          public Set<String> getAllLookupNames()
          {
            return Sets.newHashSet("billy", "bob");
          }

          @Override
          public Optional<LookupExtractorFactoryContainer> get(String lookupName)
          {
            lookupNamesPassed.add(lookupName);
            if ("existing".equals(lookupName)) {
              return Optional.of(
                  new LookupExtractorFactoryContainer("1234", new MapLookupExtractorFactory(new HashMap<>(), true))
              );
            } else if ("specializable".equals(lookupName)) {
              return Optional.of(
                  new LookupExtractorFactoryContainer("abcd", new MySpecializableExtractorFactory()
                  {
                    @Override
                    public LookupExtractorFactory specialize(LookupSpec value)
                    {
                      Assert.assertEquals(
                          new LookupSpec(
                              "specializable[one][two][three][four]",
                              "specializable",
                              "one][two][three][four"
                          ),
                          value
                      );
                      return specializedLookup;
                    }
                  })
              );
            } else {
              return Optional.empty();
            }
          }
        }
    );

    Assert.assertEquals(Sets.newHashSet("billy", "bob"), provider.getAllLookupNames());

    Assert.assertFalse(provider.get("normal").isPresent());
    Assert.assertEquals(Collections.singletonList("normal"), lookupNamesPassed);

    lookupNamesPassed.clear();
    Assert.assertFalse(provider.get("malformed]").isPresent());
    Assert.assertEquals(Collections.singletonList("malformed]"), lookupNamesPassed);

    lookupNamesPassed.clear();
    Assert.assertFalse(provider.get("notThere[one][two][three][four]").isPresent());
    Assert.assertEquals(Arrays.asList("notThere", "notThere[one][two][three][four]"), lookupNamesPassed);

    lookupNamesPassed.clear();
    Assert.assertFalse(provider.get("existing[one][two][three][four]").isPresent());
    Assert.assertEquals(Arrays.asList("existing", "existing[one][two][three][four]"), lookupNamesPassed);

    lookupNamesPassed.clear();
    final Optional<LookupExtractorFactoryContainer> container = provider.get("specializable[one][two][three][four]");
    Assert.assertTrue(container.isPresent());
    Assert.assertSame(specializedLookup, container.get().getLookupExtractorFactory());
    Assert.assertEquals(Collections.singletonList("specializable"), lookupNamesPassed);
  }

  private static class MySpecializableExtractorFactory implements LookupExtractorFactory, SpecializableLookup
  {
    @Override
    public boolean start()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean close()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean replaces(@Nullable LookupExtractorFactory other)
    {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public LookupIntrospectHandler getIntrospectHandler()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public LookupExtractor get()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public LookupExtractorFactory specialize(LookupSpec value)
    {
      throw new UnsupportedOperationException();
    }
  }
}