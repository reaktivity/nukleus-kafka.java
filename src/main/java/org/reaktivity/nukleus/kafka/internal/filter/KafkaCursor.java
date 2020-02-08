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
package org.reaktivity.nukleus.kafka.internal.filter;

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.POSITION_UNSET;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.RETRY_SEGMENT;

import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaIndexRecord;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public final class KafkaCursor
{
    private final KafkaCondition condition;

    private KafkaCacheSegment segment;
    private long offset;
    private long record;

    KafkaCursor(
        KafkaCondition condition)
    {
        this.condition = condition;
    }

    public void init(
        KafkaCacheSegment segment,
        long offset)
    {
        this.segment = segment;
        this.offset = offset;
        final long record = condition.reset(segment, offset, POSITION_UNSET);
        this.record = record == RETRY_SEGMENT || record == NEXT_SEGMENT ? 0L : record;
    }

    public KafkaCacheEntryFW next(
        KafkaCacheEntryFW cacheEntry)
    {
        KafkaCacheEntryFW nextEntry = null;
        KafkaCacheSegment nextSegment = segment;

        while (nextSegment != null && nextEntry == null)
        {
            final long nextRecord = condition.next(record);

            if (nextRecord == RETRY_SEGMENT)
            {
                break;
            }
            else if (nextRecord == NEXT_SEGMENT)
            {
                nextSegment = segment.nextSegment();
                if (nextSegment != null)
                {
                    segment = nextSegment;
                    record = condition.reset(segment, offset, POSITION_UNSET);
                }
            }
            else
            {
                final int index = KafkaIndexRecord.index(nextRecord);
                assert index >= 0;

                final int position = KafkaIndexRecord.value(nextRecord);
                assert position >= 0;

                nextEntry = segment.readLog(position, cacheEntry);
                assert nextEntry != null;

                if (!condition.test(nextEntry))
                {
                    nextEntry = null;
                }

                this.record = nextRecord;
            }
        }

        return nextEntry;
    }

    public void advance(
        long offset)
    {
        assert offset > this.offset;
        this.offset = offset;
        this.record = KafkaIndexRecord.nextIndex(KafkaIndexRecord.nextValue(record));
    }

    @Override
    public String toString()
    {
        return String.format("%s[offset %d, record %016x, segment %s, condition %s]",
                getClass().getSimpleName(), offset, record, segment, condition);
    }
}
