package io.imply.druid.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A CloseableNaivePool is a pool of closeable resources that are maintained in a pool.
 *
 * It is closely related to a StupidPool, but is slightly different.  The primary difference is that, instead of using
 * cleaners and various other things deep in the weeds, it resorts back to using the finalize() method to catch leaks.
 * It mitigates the cost of finalization by reusing the object that overrides finalize, ensuring that finalize is only
 * ever overridden on an object that is either leaked or actively closed upon closing of the Pool.
 *
 * The ObjectResourceHolder is the reused object, the ObjectResourceHolder itself is never exposed outside of this
 * class, instead a PooledResourceHolder is returned from the take() method.  The PooledResourceHolder contains a
 * version identifier of the ObjectResourceHolder that it carries a reference to.  This is used to ensure that the
 * ObjectResourceHolder is only returned to the pool once.  The PooledResourceHolder object is created on every
 * call to take() and gets GC'd, but the underlying ObjectResourceHolder should only ever be GC'd if it was leaked
 * or if the whole Pool is closed.
 *
 * @param <T>
 */
public class CloseableNaivePool<T extends Closeable> implements Closeable
{
  private static final Logger log = new Logger(CloseableNaivePool.class);

  private final String name;
  private final Supplier<T> generator;

  private final ConcurrentLinkedQueue<ObjectResourceHolder> objects;
  private final AtomicInteger numObjects;
  private final AtomicBoolean closed;

  /**
   * The max number of objects to retain in the pool.  This is very specifically only a limit on what is kept in the
   * pool and is not a limit on the number of objects created by the pool, which is infinite.
   */
  private final int objectsCacheMaxCount;

  public static <T extends Closeable> CloseableNaivePool<T> make(String name, Supplier<T> generator, int initCount, int objectsCacheMaxCount)
  {
    Preconditions.checkArgument(
        initCount <= objectsCacheMaxCount,
        "initCount[%s] must be less/equal to objectsCacheMaxCount[%s]",
        initCount,
        objectsCacheMaxCount
    );

    List<T> objects = new ArrayList<>(initCount);
    for (int i = 0; i < initCount; i++) {
      objects.add(generator.get());
    }

    return new CloseableNaivePool<>(name, generator, objects, objectsCacheMaxCount);
  }


  public CloseableNaivePool(String name, Supplier<T> generator)
  {
    this(name, generator, null, Integer.MAX_VALUE);
  }

  private CloseableNaivePool(
      String name,
      Supplier<T> generator,
      List<T> initialObjects,
      int objectsCacheMaxCount
  )
  {
    this.name = name;
    this.generator = generator;
    this.objectsCacheMaxCount = objectsCacheMaxCount;
    this.closed = new AtomicBoolean(false);

    this.objects = new ConcurrentLinkedQueue<>();
    if (initialObjects == null) {
      this.numObjects = new AtomicInteger(0);
    } else {
      for (T initialObject : initialObjects) {
        objects.offer(new ObjectResourceHolder(initialObject));
      }
      this.numObjects = new AtomicInteger(initialObjects.size());
    }
  }

  public PooledResourceHolder<T> take()
  {
    final ObjectResourceHolder obj = objects.poll();
    if (obj == null) {
      return new ObjectResourceHolder(generator.get()).makeChild();
    } else {
      numObjects.decrementAndGet();
      return obj.makeChild();
    }
  }

  @Override
  public void close()
  {
    if (closed.compareAndSet(false, true)) {
      Closer closer = Closer.create();
      ObjectResourceHolder holder;
      while ((holder = objects.poll()) != null) {
        closer.register(holder);
      }
      try {
        closer.close();
      }
      catch (IOException e) {
        log.warn(e, "Problem when closing pool [%s]", name);
      }
    }
  }

  private class ObjectResourceHolder implements ResourceHolder<T>
  {
    private final AtomicInteger version = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final T object;

    private ObjectResourceHolder(final T object)
    {
      this.object = object;
    }

    // WARNING: it is entirely possible for a caller to hold onto the object and call ObjectResourceHolder.close,
    // Then still use that object even though it will be offered to someone else in StupidPool.take
    @Override
    public T get()
    {
      if (closed.get()) {
        throw new ISE("Closed!?");
      } else {
        return object;
      }
    }

    public void returnToPool()
    {
      if (CloseableNaivePool.this.closed.get()) {
        if (!closed.get()) {
          close();
        }
        return;
      }

      final int numEntries = numObjects.getAndUpdate(operand -> operand < objectsCacheMaxCount ? operand + 1 : operand);
      if (numEntries < objectsCacheMaxCount) {
        if (!objects.offer(this)) {
          log.warn(new ISE("Queue offer failed"), "Could not offer object back to pool [%s]", name);
        }
      }
    }

    public PooledResourceHolder<T> makeChild()
    {
      return new PooledResourceHolder<>(this, version.get());
    }

    @Override
    public void close()
    {
      if (closed.compareAndSet(false, true)) {
        try {
          object.close();
        }
        catch (IOException e) {
          log.warn(e, "Problem closing object in pool [%s]", name);
        }
      } else {
        log.warn(new ISE("Already Closed!"), "Already closed");
      }
    }

    @Override
    protected void finalize() throws Throwable
    {
      // This is using a finalize to trace whether it has been leaked.  The ObjectResourceHolder will end up being
      // put back in the queue continuously.  While it is in the queue it is technically "closed"
      try {
        if (!closed.get()) {
          log.warn("Object in pool [%s] not closed! version [%d], allowing gc to prevent leak.", name, version.get());
        }
      }
      finally {
        super.finalize();
      }
    }
  }

  public static class PooledResourceHolder<T extends Closeable> implements AutoCloseable
  {
    private final CloseableNaivePool<T>.ObjectResourceHolder actualResource;
    private final int version;

    private PooledResourceHolder(
        CloseableNaivePool<T>.ObjectResourceHolder actualResource,
        int version
    )    {
      this.actualResource = actualResource;
      this.version = version;
    }

    public T get()
    {
      final int currVersion = actualResource.version.get();
      if (currVersion == this.version) {
        return actualResource.get();
      } else {
        throw new ISE("Holder closed, expected version [%d] != current version [%d]", this.version, currVersion);
      }
    }

    @Override
    public void close()
    {
      if (actualResource.version.compareAndSet(version, version + 1)) {
        actualResource.returnToPool();
      } else {
        log.warn(
            new ISE("Already Closed!? version [%d], actual version [%d]", version, actualResource.version.get()),
            "Already Closed"
        );
      }
    }
  }
}
