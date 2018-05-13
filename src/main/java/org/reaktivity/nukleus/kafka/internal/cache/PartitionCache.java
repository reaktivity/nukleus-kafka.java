/**
 * Copyright 2016-2017 The Reaktivity Project
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

import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.EMPTY_BYTE_ARRAY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;

public class PartitionCache
{
    private static final int NO_MESSAGE = -1;
    private static final int NO_POSITION = -1;

    private final MessageCache messageCache;
    private final Map<UnsafeBuffer, Entry> offsetsByKey;
    private final List<Entry> entries;
    private int compactFrom = NO_POSITION;

    private final LongIterator iterator = new LongIterator();

    private final UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    public PartitionCache(
        int initialCapacity,
        MessageCache messageCache)
    {
        this.messageCache = messageCache;
        this.offsetsByKey = new HashMap<>(initialCapacity);
        entries = new ArrayList<Entry>(initialCapacity);
    }

    public void add(
        long requestOffset,
        long nextFetchOffset,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value)
    {
        long messageOffset = nextFetchOffset - 1;
        long highestOffset = entries.get(entries.size() - 1).offset;
        Entry entry = null;

        if (messageOffset > highestOffset)
        {
            buffer.wrap(key, 0, key.capacity());
            if (value == null)
            {
                entry = offsetsByKey.remove(buffer);
                messageCache.release(entry.message);
                compactFrom = Math.min(compactFrom, entry.position);
                entry.position = NO_POSITION;
            }
            else
            {
                entry = offsetsByKey.get(buffer);
                if (entry == null)
                {
                    UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.capacity()]);
                    keyCopy.putBytes(0,  key, 0, key.capacity());
                    entry = new Entry(messageOffset, entries.size(), NO_MESSAGE);
                    offsetsByKey.put(keyCopy, entry);
                }
                else
                {
                    if (messageOffset > entry.offset)
                    {
                        compactFrom = Math.min(compactFrom, entry.position);
                        entry.position = entries.size();
                        entry.offset = messageOffset;
                        entries.add(entry);
                    }
                }
                entry.message = messageCache.put(timestamp, traceId, key, headers, value);
            }
        }
        else
        {
            // cache the message if it was previously evicted due to lack of space
            buffer.wrap(key, 0, key.capacity());
            entry = offsetsByKey.get(buffer);
            if (entry.message == NO_MESSAGE)
            {
                // TODO: only cache if the message was of interest to at least one dispatcher
                //       (call next dispatcher)
                entry.message = messageCache.put(timestamp, traceId, key, headers, value);
            }
        }
    }

    public void compact()
    {
        if (compactFrom != NO_POSITION)
        {
            int ceiling = NO_POSITION;
            for (int i=0; i < entries.size(); i++)
            {
                Entry entry = entries.get(i);
                if (entry.position != i)
                {
                    if (ceiling == NO_POSITION)
                    {
                        ceiling = i;
                    }
                }
                else if (ceiling != NO_POSITION)
                {
                    entry.position = ceiling;
                    entries.set(ceiling, entry);
                    ceiling++;
                }
            }
            if (ceiling > entries.size())
            {

            }
            compactFrom = NO_POSITION;
        }
    }

    public LongIterator offsets(
        long startOffset)
    {
        iterator.position = locate(startOffset);
        return iterator;
    }

    private int locate(
        long offset)
    {
        compact();
        Entry candidate = new Entry(offset, NO_POSITION, NO_MESSAGE);
        return Collections.binarySearch(entries, candidate);
    }

    public final class LongIterator
    {
        private int position;

        public boolean hasNext()
        {
            return position < entries.size();
        }

        public long next()
        {
            return entries.get(position++).offset;
        }
    }

    private static final class Entry implements Comparable<Entry>
    {
        long offset;
        int  position;
        int  message;

        private  Entry(
            long offset,
            int  position,
            int  message
            )
        {
            this.offset = offset;
            this.position = position;
            this.message = message;
        }

        @Override
        public int compareTo(
            Entry o)
        {
            return (int) (this.offset - o.offset);
        }
    }

}
