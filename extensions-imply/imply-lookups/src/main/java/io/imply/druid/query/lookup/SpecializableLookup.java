/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.lookup;

import org.apache.druid.query.lookup.LookupExtractorFactory;

import javax.annotation.Nullable;
import java.util.Objects;

public interface SpecializableLookup
{
  LookupExtractorFactory specialize(LookupSpec value);

  class LookupSpec
  {
    @Nullable
    public static LookupSpec parseString(String lookupName)
    {
      if (lookupName.endsWith("]")) {
        // We might have a lookup that we want to specialize, so let's look for the base lookup.
        // At various points, if we fail to get what we want with the complex name, we fall through to
        // logic that just passes it through un-tampered to the delegate.

        int firstSquareBracket = lookupName.indexOf('[');
        if (firstSquareBracket <= 0) {
          return null;
        }

        return new LookupSpec(
            lookupName,
            lookupName.substring(0, firstSquareBracket),
            lookupName.substring(firstSquareBracket + 1, lookupName.length() - 1)
        );
      }
      return null;
    }

    private final String theString;
    private final String lookupName;
    private final String valInBrackets;

    public LookupSpec(String theString, String lookupName, String valInBrackets)
    {
      this.theString = theString;
      this.lookupName = lookupName;
      this.valInBrackets = valInBrackets;
    }

    public String getTheString()
    {
      return theString;
    }

    public String getLookupName()
    {
      return lookupName;
    }

    public String getValInBrackets()
    {
      return valInBrackets;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof LookupSpec)) {
        return false;
      }
      LookupSpec that = (LookupSpec) o;
      return Objects.equals(theString, that.theString)
             && Objects.equals(lookupName, that.lookupName)
             && Objects.equals(valInBrackets, that.valInBrackets);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(theString, lookupName, valInBrackets);
    }

    @Override
    public String toString()
    {
      return "LookupSpec{" +
             "theString='" + theString + '\'' +
             ", lookupName='" + lookupName + '\'' +
             ", valInBrackets='" + valInBrackets + '\'' +
             '}';
    }
  }
}
