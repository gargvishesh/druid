/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.theta;

import org.apache.datasketches.Family;
import org.apache.datasketches.ResizeFactor;
import org.apache.datasketches.memory.WritableMemory;

/**
 * {@link HeapQuickSelectSketch} doesn't publicaly provide a way to update it directly with a hashed value - where the
 * hashed value is equivalent to what the sketch would've also calculated on its own.
 * We needed that functionality since we're calculating the hashes outside the sketch to re-use it at multiple places
 * including the sketch itself. Hence, we've extended the HeapQuickSelectSketch class to use its package-private method
 * {@link HeapQuickSelectSketch#hashUpdate(long)}.
 */
public class RawHashHeapQuickSelectSketch extends HeapQuickSelectSketch
{
  public RawHashHeapQuickSelectSketch(int lgNomLongs, long seed, float p, ResizeFactor rf, boolean unionGadget)
  {
    super(lgNomLongs, seed, p, rf, unionGadget);
  }

  public void updateHashes(long[] hashes, int start, int end)
  {
    for (int i = start; i < end; i++) {
      hashUpdate(hashes[i]);
    }
  }

  public void updateHash(long hash)
  {
    hashUpdate(hash);
  }

  public static RawHashHeapQuickSelectSketch create(int nominalEntries, long seed)
  {
    UpdateSketch sketch = new UpdateSketchBuilder()
        .setFamily(Family.QUICKSELECT)
        .setNominalEntries(nominalEntries)
        .setSeed(seed)
        .build();

    return new RawHashHeapQuickSelectSketch(
        sketch.getLgNomLongs(),
        sketch.getSeed(),
        sketch.getP(),
        sketch.getResizeFactor(),
        false
    );
  }

  // this is an unnecessary method added to keep dependency analyzer happy. If this change is not present, the analyzer
  // throws exception saying datasketches-memory is a unused but declared dependency. It is weird in the sense
  // that if datasketches-memory is not marked compile time, then the compilation itself also fails.
  @Override
  WritableMemory getMemory()
  {
    return super.getMemory();
  }
}
