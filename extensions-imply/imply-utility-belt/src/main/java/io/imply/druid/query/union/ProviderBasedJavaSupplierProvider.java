/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.query.union;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;

import java.util.function.Supplier;


/**
 * A Provider of a Supplier that uses a Provider to implement the Supplier.
 */
public class ProviderBasedJavaSupplierProvider<T> implements Provider<Supplier<T>>
{
  private final Key<T> supplierKey;
  private Provider<T> instanceProvider;

  public ProviderBasedJavaSupplierProvider(
      Key<T> instanceKey
  )
  {
    this.supplierKey = instanceKey;
  }

  @Inject
  public void configure(Injector injector)
  {
    this.instanceProvider = injector.getProvider(supplierKey);
  }


  @Override
  public Supplier<T> get()
  {
    return instanceProvider::get;
  }
}
