/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.lookup;

import com.google.inject.Inject;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainer;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;

import java.util.Optional;
import java.util.Set;

public class ImplyLookupProvider implements LookupExtractorFactoryContainerProvider
{
  private final LookupExtractorFactoryContainerProvider delegate;

  @Inject
  public ImplyLookupProvider(
      @ImplyLookup LookupExtractorFactoryContainerProvider delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public Set<String> getAllLookupNames()
  {
    return delegate.getAllLookupNames();
  }

  @Override
  public Optional<LookupExtractorFactoryContainer> get(String lookupName)
  {
    final SpecializableLookup.LookupSpec lookupSpec = SpecializableLookup.LookupSpec.parseString(lookupName);
    if (lookupSpec == null) {
      return delegate.get(lookupName);
    } else {
      // We might have a lookup that we want to specialize, so let's look for the base lookup.
      // At various points, if we fail to get what we want with the complex name, we fall through to
      // logic that just passes it through un-tampered to the delegate.

      final LookupExtractorFactoryContainer container = delegate.get(lookupSpec.getLookupName()).orElse(null);

      if (container == null) {
        return delegate.get(lookupName);
      }

      LookupExtractorFactory factory = container.getLookupExtractorFactory();
      if (factory instanceof SpecializableLookup) {
        final LookupExtractorFactory specializedFactory = ((SpecializableLookup) factory).specialize(lookupSpec);
        return Optional.of(new LookupExtractorFactoryContainer(container.getVersion(), specializedFactory));
      }

      return delegate.get(lookupName);
    }
  }
}
