package io.imply.druid.query.lookup;

import org.apache.druid.query.lookup.LookupExtractorFactory;

public interface SpecializableLookup
{
  LookupExtractorFactory specialize(String value);
}
