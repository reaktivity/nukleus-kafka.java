/**
 * Copyright 2016-2020 The Reaktivity Project
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

import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSentinelSegment;

public final class KafkaCachePartitionReader
{
    private final int partitionId;
    private final KafkaCacheSentinelSegment sentinel;

    KafkaCachePartitionReader(
        int partitionId,
        KafkaCacheSentinelSegment sentinel)
    {
        this.partitionId = partitionId;
        this.sentinel = sentinel;
    }

    public int id()
    {
        return partitionId;
    }

    public KafkaCacheSegment seekNotAfter(
        long offset)
    {
        KafkaCacheSegment segment = sentinel.headSegment();

        while (segment != sentinel && segment.baseOffset() > offset)
        {
            segment = segment.previousSegment;
        }

        return segment;
    }

    public KafkaCacheSegment seekNotBefore(
        long offset)
    {
        KafkaCacheSegment segment = sentinel;

        while (segment != null && segment.baseOffset() < offset)
        {
            segment = segment.nextSegment;
        }

        return segment;
    }
}
