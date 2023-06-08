package io.imply.druid.query.lookup;

import com.google.inject.Inject;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.timeline.DataSegment;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

public class SegmentManagerWithCallbacks extends SegmentManager
{
  private final ConcurrentHashMap<String, CopyOnWriteArrayList<Runnable>> callbacks = new ConcurrentHashMap<>();

  @Inject
  public SegmentManagerWithCallbacks(SegmentLoader segmentLoader)
  {
    super(segmentLoader);
  }

  public void registerCallback(String datasource, Runnable callback)
  {
    callbacks.computeIfAbsent(datasource, (s) -> new CopyOnWriteArrayList<>()).add(callback);
  }

  public void unregisterCallback(String datasource, Runnable callback)
  {
    final CopyOnWriteArrayList<Runnable> runnables = callbacks.get(datasource);
    if (runnables != null) {
      // Intentionally unregister based on reference equality
      //noinspection ObjectEquality
      runnables.removeIf((obj) -> obj == callback);
    }
  }

  @Override
  public boolean loadSegment(
      DataSegment segment,
      boolean lazy,
      SegmentLazyLoadFailCallback loadFailed,
      ExecutorService loadSegmentIntoPageCacheExec
  ) throws SegmentLoadingException
  {
    final boolean retVal = super.loadSegment(segment, lazy, loadFailed, loadSegmentIntoPageCacheExec);
    if (retVal) {
      final CopyOnWriteArrayList<Runnable> callbacks = this.callbacks.get(segment.getDataSource());
      if (callbacks != null) {
        for (Runnable callback : callbacks) {
          callback.run();
        }
      }
    }
    return retVal;
  }
}
