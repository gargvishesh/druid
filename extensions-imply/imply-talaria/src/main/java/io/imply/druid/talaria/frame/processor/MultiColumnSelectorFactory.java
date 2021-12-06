/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.talaria.frame.processor;

import com.google.common.base.Predicate;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

public class MultiColumnSelectorFactory implements ColumnSelectorFactory
{
  private final List<Supplier<ColumnSelectorFactory>> factorySuppliers;
  private final ColumnInspector columnInspector;

  private int currentFactory = 0;

  public MultiColumnSelectorFactory(
      final List<Supplier<ColumnSelectorFactory>> factorySuppliers,
      final ColumnInspector columnInspector
  )
  {
    this.factorySuppliers = factorySuppliers;
    this.columnInspector = columnInspector;
  }

  public void setCurrentFactory(final int currentFactory)
  {
    this.currentFactory = currentFactory;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return new DimensionSelector()
    {
      private final ColumnSelectorFactory[] delegateFactories = new ColumnSelectorFactory[factorySuppliers.size()];
      private final DimensionSelector[] delegateSelectors = new DimensionSelector[factorySuppliers.size()];

      @Override
      public IndexedInts getRow()
      {
        return populateDelegate().getRow();
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
      }

      @Override
      public ValueMatcher makeValueMatcher(Predicate<String> predicate)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return populateDelegate().getObject();
      }

      @Override
      public Class<?> classOfObject()
      {
        return populateDelegate().classOfObject();
      }

      @Override
      public int getValueCardinality()
      {
        return CARDINALITY_UNKNOWN;
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return populateDelegate().lookupName(id);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return populateDelegate().lookupNameUtf8(id);
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        return populateDelegate().supportsLookupNameUtf8();
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return false;
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return null;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Do nothing.
      }

      private DimensionSelector populateDelegate()
      {
        final ColumnSelectorFactory factory = factorySuppliers.get(currentFactory).get();

        //noinspection ObjectEquality: checking reference equality intentionally
        if (factory != delegateFactories[currentFactory]) {
          delegateSelectors[currentFactory] = factory.makeDimensionSelector(dimensionSpec);
          delegateFactories[currentFactory] = factory;
        }

        return delegateSelectors[currentFactory];
      }
    };
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(final String columnName)
  {

    return new ColumnValueSelector()
    {
      private final ColumnSelectorFactory[] delegateFactories = new ColumnSelectorFactory[factorySuppliers.size()];

      @SuppressWarnings("rawtypes")
      private final ColumnValueSelector[] delegateSelectors = new ColumnValueSelector[factorySuppliers.size()];

      @Override
      public double getDouble()
      {
        return populateDelegate().getDouble();
      }

      @Override
      public float getFloat()
      {
        return populateDelegate().getFloat();
      }

      @Override
      public long getLong()
      {
        return populateDelegate().getLong();
      }

      @Override
      public boolean isNull()
      {
        return populateDelegate().isNull();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return populateDelegate().getObject();
      }

      @Override
      public Class classOfObject()
      {
        // Assumes all delegate factories have the same class of object.
        return populateDelegate().classOfObject();
      }

      private ColumnValueSelector<?> populateDelegate()
      {
        final ColumnSelectorFactory factory = factorySuppliers.get(currentFactory).get();

        //noinspection ObjectEquality: checking reference equality intentionally
        if (factory != delegateFactories[currentFactory]) {
          delegateSelectors[currentFactory] = factory.makeColumnValueSelector(columnName);
          delegateFactories[currentFactory] = factory;
        }

        return delegateSelectors[currentFactory];
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Do nothing.
      }
    };
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return columnInspector.getColumnCapabilities(column);
  }
}
