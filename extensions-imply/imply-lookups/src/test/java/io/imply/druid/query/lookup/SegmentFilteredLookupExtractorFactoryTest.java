package io.imply.druid.query.lookup;

import io.imply.druid.util.CronFactory;
import io.imply.druid.util.TestScheduledExecutorService;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("ALL")
public class SegmentFilteredLookupExtractorFactoryTest
{
  private AtomicBoolean scheduled;
  private SegmentManagerWithCallbacks segs;
  private CronFactory cronFactory;

  @Before
  public void setUp() throws Exception
  {
    scheduled = new AtomicBoolean(false);
    final ScheduledExecutorService myExecutor = new TestScheduledExecutorService(){
      @Override
      public ScheduledFuture<?> schedule(
          Runnable command, long delay, TimeUnit unit
      )
      {
        Assert.assertEquals(0, delay);
        Assert.assertEquals(TimeUnit.MILLISECONDS, unit);
        scheduled.set(true);
        // We count on the callback to load it for the tests instead of the schedule, so just ignore the scheduling
        return null;
      }

      @Override
      public <T> Future<T> submit(Callable<T> task)
      {
        try {
          task.call();
        }
        catch (Exception e) {
          throw new RE(e);
        }
        return null;
      }
    };
    segs = new SegmentManagerWithCallbacks(new SegmentFilterLookupTestsLoader());
    cronFactory = new CronFactory(myExecutor)
    {
      @Override
      public void scheduleOnce(Runnable runnable, Duration duration)
      {
        runnable.run();
      }
    };
  }

  @Test
  public void testVariousErrorScenarios() throws SegmentLoadingException
  {
    SegmentFilteredLookupExtractorFactory factory = buildNonFilteredFactory();

    try {
      Assert.assertTrue(factory.start());
      Assert.assertTrue(scheduled.get());

      Assert.assertThrows(UOE.class, factory::get);

      Assert.assertThrows(
          IAE.class,
          () -> factory.specialize(SpecializableLookup.LookupSpec.parseString("lookupName[colA]"))
      );

      Assert.assertThrows(
          IAE.class,
          () -> factory.specialize(SpecializableLookup.LookupSpec.parseString("lookupName[colA][colB][filter1]"))
      );

      Assert.assertEquals(
          "NullLookupExtractorFactory",
          factory.specialize(SpecializableLookup.LookupSpec.parseString("lookupName[colA][colB]"))
                 .getClass()
                 .getSimpleName()
      );

      loadLookup("nonFiltered");

      Assert.assertThrows(
          IAE.class,
          () -> factory.specialize(SpecializableLookup.LookupSpec.parseString("lookupName[notExist][colB]"))
      );

      Assert.assertThrows(
          IAE.class,
          () -> factory.specialize(SpecializableLookup.LookupSpec.parseString("lookupName[colA][notExist]"))
      );
    } finally {
      factory.close();
    }
  }

  @Test
  public void testNonFilteredLookup() throws SegmentLoadingException
  {
    SegmentFilteredLookupExtractorFactory factory = buildNonFilteredFactory();

    try {
      Assert.assertTrue(factory.start());
      Assert.assertTrue(scheduled.get());

      loadLookup("nonFiltered");

      final LookupExtractor lookup = factory.specialize(
          SpecializableLookup.LookupSpec.parseString("lookupName[colA][colB]")
      ).get();

      Assert.assertEquals("bob", lookup.apply("A"));
      Assert.assertEquals("bill", lookup.apply("B"));
      Assert.assertEquals("the", lookup.apply("C"));
      Assert.assertEquals("kid", lookup.apply("D"));
      Assert.assertNull(lookup.apply("notThere"));

      final LookupExtractor anotherLookup = factory.specialize(
          SpecializableLookup.LookupSpec.parseString("lookupName[colA][colC]")
      ).get();

      Assert.assertEquals("sally", anotherLookup.apply("A"));
      Assert.assertEquals("sue", anotherLookup.apply("B"));
      Assert.assertEquals("the", anotherLookup.apply("C"));
      Assert.assertEquals("seamstress", anotherLookup.apply("D"));
      Assert.assertNull(anotherLookup.apply("notThere"));

      final LookupExtractor yetAnotherLookup = factory.specialize(
          SpecializableLookup.LookupSpec.parseString("lookupName[colB][colC]")
      ).get();

      Assert.assertEquals("sally", yetAnotherLookup.apply("bob"));
      Assert.assertEquals("sue", yetAnotherLookup.apply("bill"));
      Assert.assertEquals("the", yetAnotherLookup.apply("the"));
      Assert.assertEquals("seamstress", yetAnotherLookup.apply("kid"));
      Assert.assertNull(yetAnotherLookup.apply("notThere"));
    } finally {
      factory.close();
    }
  }

  @Test
  public void testFilteredLookup() throws SegmentLoadingException
  {
    SegmentFilteredLookupExtractorFactory factory = buildFilterableFactory();

    try {
      Assert.assertTrue(factory.start());
      Assert.assertTrue(scheduled.get());

      loadLookup("filterable");

      final LookupExtractor lookup = factory.specialize(
          SpecializableLookup.LookupSpec.parseString("lookupName[colB][colC][B]")
      ).get();

      Assert.assertEquals("bob", lookup.apply("a"));
      Assert.assertEquals("bill", lookup.apply("b"));
      Assert.assertEquals("the", lookup.apply("c"));
      Assert.assertEquals("kid", lookup.apply("d"));
      Assert.assertNull(lookup.apply("notThere"));

      final LookupExtractor anotherLookup = factory.specialize(
          SpecializableLookup.LookupSpec.parseString("lookupName[colB][colC][A]")
      ).get();

      Assert.assertEquals("sally", anotherLookup.apply("a"));
      Assert.assertEquals("sue", anotherLookup.apply("b"));
      Assert.assertEquals("the", anotherLookup.apply("c"));
      Assert.assertEquals("seamstress", anotherLookup.apply("d"));
      Assert.assertNull(anotherLookup.apply("notThere"));

      final LookupExtractor yetAnotherLookup = factory.specialize(
          SpecializableLookup.LookupSpec.parseString("lookupName[colB][colC][C]")
      ).get();

      Assert.assertEquals("three", yetAnotherLookup.apply("a"));
      Assert.assertEquals("two", yetAnotherLookup.apply("b"));
      Assert.assertEquals("one", yetAnotherLookup.apply("c"));
      Assert.assertThrows(IAE.class, () -> yetAnotherLookup.apply("d"));
      Assert.assertNull(yetAnotherLookup.apply("notThere"));
    } finally {
      factory.close();
    }
  }

  @Nonnull
  private SegmentFilteredLookupExtractorFactory buildNonFilteredFactory()
  {
    return new SegmentFilteredLookupExtractorFactory(
        "nonFiltered", Collections.emptyList(), segs, cronFactory
    );
  }

  @Nonnull
  private SegmentFilteredLookupExtractorFactory buildFilterableFactory()
  {
    return new SegmentFilteredLookupExtractorFactory(
        "filterable", Collections.singletonList("colA"), segs, cronFactory
    );
  }

  private void loadLookup(String dataSource) throws SegmentLoadingException
  {
    segs.loadSegment(
        DataSegment.builder().dataSource(dataSource).interval(Intervals.ETERNITY).size(1).version("123").build(),
        false,
        null
    );
  }
}