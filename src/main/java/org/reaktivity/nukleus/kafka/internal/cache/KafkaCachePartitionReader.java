/**
 * Copyright 2016-2019 The Reaktivity Project
 *
 * The Reaktivity Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.reaktivity.nukleus.kafka.internal.cache;

import static java.util.Collections.singleton;

import java.util.NavigableSet;
import java.util.TreeSet;

public final class KafkaCachePartitionReader
{
    private final KafkaCacheSegment.Candidate candidate = new KafkaCacheSegment.Candidate();

    private final int partitionId;
    private final NavigableSet<KafkaCacheSegment> segments;

    private KafkaCacheSegment latest;

    KafkaCachePartitionReader(
        int partitionId,
        KafkaCacheSegment segment)
    {
        this.partitionId = partitionId;
        this.segments = new TreeSet<>(singleton(segment));
        this.latest = segment;
    }

    public int id()
    {
        return partitionId;
    }

    public KafkaCacheSegment seek(
        long offset)
    {
        KafkaCacheSegment nextSegment = latest.nextSegment();
        while (nextSegment != null)
        {
            segments.add(nextSegment);
            latest = nextSegment;
            nextSegment = latest.nextSegment();
        }

        candidate.baseOffset(offset);
        return segments.floor(candidate);
    }
}
