<!--
  ~ Copyright (c) Imply Data, Inc. All rights reserved.
  ~
  ~ This software is the confidential and proprietary information
  ~ of Imply Data, Inc. You shall not disclose such Confidential
  ~ Information and shall use it only in accordance with the terms
  ~ of the license agreement you entered into with Imply.
  -->

# Concurrency model

## Threads 
Following are different execution threads in the system that can change the state

- Segment load/drop handler threads
    - create a new segment in the state.
    - delete a segment from state (begin the cleanup but do not remove the segment in same thread).
    - register a callback to be invoked when segment is no longer active.
- Download threads
    - download a queued segment.
    - evict a segment.
    - cancel a queued segment from downloading.
    - invoke callbacks waiting on the download of a segment that will in turn 
        - could access virtual segment content to prepare a query runner for that segment.
        - submit a query execution task for that segment.
- virtual segment cleanup threads
    - these threads will finish the cleanup by checking that segment is no longer active and remove any state pertaining
    to that segment.
- HTTP threads receiving query reqeusts
    - acquire a new reference on a virtual segment.
    - could release a reference on a virtual segment if the query for the segment is cancelled.  
    - schedule a virtual segment to be downloaded.
    - if the segment is already downloaded 
        - could access virtual segment content.
        - submit a query execution task for that segment.
- segment processing threads
    - release the reference on a virtual segment that was acquired while scheduling a download.
    - acquire and release reference on the virtual segment.
    - access virtual segment contents.
    
## State manager
As seen in previous sections, there are different threads accessing and operation on a virtual segment in all different
kind of ways and introducing a lot of complexity. To reduce this complexity, we have a state manager for virtual segments
that guarantees serial execution for a given virtual segment. The idea is borrowed from SegmentLocalCacheManager and 
uses a ConcurrentHashMap internally. However, there are still some operations that happen outside the state manager, 
specifically any operation that is not keyed on a segment e.g. get a next segment to download, get a next segment to evict. 

These operations are implemented in VirtualSegmentLoader and protected via a lock. We ensure that we are not doing any 
IO or blocking activity innside the lock. So, for example, you find which segment to download inside the lock but do the 
actual download outside it.

## VirtualReferenceCountingSegment

There are operations happening on VirtualReferenceCountingSegment as well. From concurrency perspective, we only care about
references. They are either incremented or decremented but that can result in callbacks getting invoked. It is 
to be ensured that callbacks are asynchronous and fast to avoid deadlocks. These callbacks are only invoked when 
reference count becomes zero and segment has no active queries. 

References can be release/acquired by different threads in the system. We ensure that the reference is acquired when 
the query first arrives. This reference is acquired inside the state manager and is released only after the query on the segment
has been run. We also ensure that any callback on references becoming zero must be completed through state manager state 
change. Since state manager is executing operations on a segment sequentially, it can ensure that no new references get
acquired after the callbacks begin executing. Code halts any state change or throws an exception if a new reference is acquired. 
Otherwise, it can make the necessary change. Refer to segment download cancellation, eviction or hard removal for how it
is being done.

Since a segment is evicted only when all the active references are closed, it needs to be ensured that there are no
dangling references that could result in a leak. 