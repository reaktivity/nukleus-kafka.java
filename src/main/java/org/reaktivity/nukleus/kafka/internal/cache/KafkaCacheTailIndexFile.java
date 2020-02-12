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

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.record;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.value;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.NEXT_SEGMENT;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheTailSegment;

public abstract class KafkaCacheTailIndexFile extends KafkaCacheTailFile
{
    protected KafkaCacheTailIndexFile(
        KafkaCacheTailSegment segment,
        String extension)
    {
        super(segment, extension);
    }

    protected Int2IntHashMap toMap()
    {
        Int2IntHashMap map = new Int2IntHashMap(-1);
        for (int index = 0; index < readCapacity; index += Long.BYTES)
        {
            long entry = readableBuf.getLong(index);
            int key = (int)(entry >>> 32);
            int value = (int)(entry & 0xffff_ffffL);
            map.put(key, value);
        }
        return map;
    }

    protected long seekKey(
        int key)
    {
        // assumes sorted by key
        final DirectBuffer buffer = readableBuf;
        final int lastIndex = (readCapacity >> 3) - 1;

        int lowIndex = 0;
        int highIndex = lastIndex;

        while (lowIndex <= highIndex)
        {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final long entry = buffer.getLong(midIndex << 3);
            final int entryKey = (int)(entry >>> 32);

            if (entryKey < key)
            {
                lowIndex = midIndex + 1;
            }
            else if (entryKey > key)
            {
                highIndex = midIndex - 1;
            }
            else
            {
                break;
            }
        }

        assert lowIndex >= 0;
        if (lowIndex <= lastIndex)
        {
            final long entry = buffer.getLong(lowIndex << 3);
            return record(lowIndex, value(entry));
        }

        return NEXT_SEGMENT;
    }

    protected long scanKey(
        int key,
        long record)
    {
        // assumes sorted by key, repeated keys, record from seekKey
        // TODO: optimize scanKey to break loop on key mismatch
        //       requires cursor condition retain memento of last match index
        final int index = KafkaCacheCursorRecord.index(record);
        final int value = KafkaCacheCursorRecord.value(record);
        assert index >= 0;

        final DirectBuffer buffer = readableBuf;
        final int capacity = readCapacity;
        final int lastIndex = (capacity >> 3) - 1;

        int currentIndex = index;
        while (currentIndex <= lastIndex)
        {
            final long entry = buffer.getLong(currentIndex << 3);
            final int entryKey = (int)(entry >>> 32);
            final int entryValue = (int)(entry & 0x7FFF_FFFF);
            if (entryKey == key && entryValue >= value)
            {
                break;
            }
            currentIndex++;
        }

        if (currentIndex <= lastIndex)
        {
            final long entry = buffer.getLong(currentIndex << 3);
            return record(currentIndex, value(entry));
        }

        return NEXT_SEGMENT;
    }

    protected long scanIndex(
        long record)
    {
        final int index = KafkaCacheCursorRecord.index(record);
        assert index >= 0;

        final DirectBuffer buffer = readableBuf;
        final int lastIndex = (readCapacity >> 3) - 1;

        if (index <= lastIndex)
        {
            final long entry = buffer.getLong(index << 3);
            return record(index, value(entry));
        }

        return NEXT_SEGMENT;
    }
}
