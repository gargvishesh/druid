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

public class SimpleTimeSeriesSerdeTestCases
{
  private static final MethodCallCapturer<BytesReadWriteTest> METHOD_CALL_CAPTURER =
      new MethodCallCapturer<>(BytesReadWriteTest.class);

  private static TestMethodHandle capture(MethodAccess<BytesReadWriteTest, Exception> access)
  {
    try {
      Method method = METHOD_CALL_CAPTURER.captureMethod(access);
      TestMethodHandle testMethodHandle = new TestMethodHandle(method.getName());

      return testMethodHandle;
    }
    catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  private final Map<TestMethodHandle, Integer> testCasesToRun = new LinkedHashMap<>();

  public SimpleTimeSeriesSerdeTestCases setTestCaseExpectedValue(TestMethodHandle testMethodHandle, int expectedSize)
  {
    testCasesToRun.put(testMethodHandle, expectedSize);

    return this;
  }

  public SimpleTimeSeriesSerdeTestCases setTestCaseExpectedValue(
      MethodAccess<BytesReadWriteTest, Exception> methodAccess,
      int expectedSize
  )
  {
    TestMethodHandle testMethodHandle = capture(methodAccess);
    testCasesToRun.put(testMethodHandle, expectedSize);

    return this;
  }

  public SimpleTimeSeriesSerdeTestCases enableTestCase(MethodAccess<BytesReadWriteTest, Exception> methodAccess)
  {
    TestMethodHandle testMethodHandle = capture(methodAccess);
    testCasesToRun.put(testMethodHandle, -1);

    return this;
  }

  public int currentTestValue()
  {
    TestMethodHandle currentTestMethodHandle = getCurrentTestMethod();
    return testCasesToRun.get(currentTestMethodHandle);
  }

  public boolean currentTestEnabled()
  {
    TestMethodHandle currentTestMethodHandle = getCurrentTestMethod();
    return testCasesToRun.containsKey(currentTestMethodHandle);
  }

  private TestMethodHandle getCurrentTestMethod()
  {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    String thisMethodName = stackTrace[3].getMethodName();

    return new TestMethodHandle(thisMethodName);
  }

  public static class TestMethodHandle
  {
    private static final Class<?> TEST_CLASS_INTERFACE = BytesReadWriteTest.class;
    private static final Class<?> TEST_CLASS_IMPL = BytesReadWriteTestBase.class;

    private final String name;

    public TestMethodHandle(String name)
    {
      this.name = name;
      try {
        // validate method exists
        MethodHandles.lookup()
                     .findVirtual(TEST_CLASS_INTERFACE, name, MethodType.methodType(void.class));
        // validate method exists
        MethodHandles.lookup()
                     .findVirtual(TEST_CLASS_IMPL, name, MethodType.methodType(void.class));
      }
      catch (NoSuchMethodException | IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }

    public String getName()
    {
      return TEST_CLASS_INTERFACE.getName() + "::void " + name + "()";
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

  public static class MethodCallCapturer<T> implements InvocationHandler
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
