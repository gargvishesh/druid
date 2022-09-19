/*
 * Copyright (c) Imply Data, Inc. All rights reserved.
 *
 * This software is the confidential and proprietary information
 * of Imply Data, Inc. You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms
 * of the license agreement you entered into with Imply.
 */

package io.imply.druid.segment.serde.simpletimeseries;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestCasesConfig<T>
{
  private final MethodCallCapturer<T> methodCallCapturer;
  private final Class<T> testCasesInterface;
  private final Class<? extends T> testClassImpl;
  private final Map<TestMethodHandle, TestCaseResult> testCasesToRun = new LinkedHashMap<>();

  public TestCasesConfig(Class<T> testCasesInterface, Class<? extends T> testClassImpl)
  {
    methodCallCapturer = new MethodCallCapturer<>(testCasesInterface);
    this.testCasesInterface = testCasesInterface;
    this.testClassImpl = testClassImpl;
  }

  public TestCasesConfig<T> setTestCaseValue(TestMethodHandle testMethodHandle, TestCaseResult expectedResult)
  {
    testCasesToRun.put(testMethodHandle, expectedResult);

    return this;
  }

  public TestCasesConfig<T> setTestCaseValue(MethodAccess<T, Exception> methodAccess, TestCaseResult expectedResult)
  {
    TestMethodHandle testMethodHandle = capture(methodAccess);
    testCasesToRun.put(testMethodHandle, expectedResult);

    return this;
  }

  public TestCasesConfig<T> setTestCaseValue(MethodAccess<T, Exception> methodAccess, int sizeBytes)
  {
    TestMethodHandle testMethodHandle = capture(methodAccess);
    testCasesToRun.put(testMethodHandle, TestCaseResult.of(sizeBytes));

    return this;
  }

  public TestCasesConfig<T> setTestCaseValue(MethodAccess<T, Exception> methodAccess, byte[] bytes)
  {
    TestMethodHandle testMethodHandle = capture(methodAccess);
    testCasesToRun.put(testMethodHandle, TestCaseResult.of(bytes));

    return this;
  }

  public TestCasesConfig<T> enableTestCase(MethodAccess<T, Exception> methodAccess)
  {
    TestMethodHandle testMethodHandle = capture(methodAccess);
    testCasesToRun.put(testMethodHandle, TestCaseResult.of(-1));

    return this;
  }

  public TestCaseResult currentTestValue()
  {
    TestMethodHandle currentTestMethodHandle = getCurrentTestMethod();
    return testCasesToRun.get(currentTestMethodHandle);
  }

  public boolean isCurrentTestEnabled()
  {
    TestMethodHandle currentTestMethodHandle = getCurrentTestMethod();
    return testCasesToRun.containsKey(currentTestMethodHandle);
  }

  private TestMethodHandle capture(MethodAccess<T, Exception> access)
  {
    try {
      Method method = methodCallCapturer.captureMethod(access);
      TestMethodHandle testMethodHandle = new TestMethodHandle(method.getName());

      return testMethodHandle;
    }
    catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  private TestMethodHandle getCurrentTestMethod()
  {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    String thisMethodName = stackTrace[3].getMethodName();

    return new TestMethodHandle(thisMethodName);
  }

  public class TestMethodHandle
  {
    private final String name;

    public TestMethodHandle(String name)
    {
      this.name = name;
      try {
        // validate method exists
        MethodHandles.lookup()
                     .findVirtual(testCasesInterface, name, MethodType.methodType(void.class));
        // validate method exists
        MethodHandles.lookup()
                     .findVirtual(testClassImpl, name, MethodType.methodType(void.class));
      }
      catch (NoSuchMethodException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    public String getName()
    {
      return testCasesInterface.getName() + "::void " + name + "()";
    }

    @Override
    public int hashCode()
    {
      return getName().hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj != null && this.getClass().equals(obj.getClass())) {
        return getName().equals(((TestMethodHandle) obj).getName());
      }

      return false;
    }


    @Override
    public String toString()
    {
      return getName();
    }
  }

  public interface MethodAccess<I, T extends Throwable>
  {
    void access(I input) throws T;
  }

  private static class MethodCallCapturer<T> implements InvocationHandler
  {
    private volatile Method lastMethod = null;
    private final T wrapper;

    @SuppressWarnings("unchecked")
    public MethodCallCapturer(Class<T> clazz)
    {
      wrapper = (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class[]{clazz}, this);
    }

    public <E extends Throwable> Method captureMethod(MethodAccess<T, E> access) throws Throwable
    {
      access.access(wrapper);

      return lastMethod;
    }


    @SuppressWarnings("ReturnOfNull")
    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
    {
      lastMethod = method;

      // unused
      return null;
    }
  }
}
