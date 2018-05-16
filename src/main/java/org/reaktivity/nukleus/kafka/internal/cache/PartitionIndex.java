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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class PartitionIndex
{
    public static final int TOMBSTONE_MESSAGE = -2;

    private static final int NO_MESSAGE = MessageCache.NO_MESSAGE;
    private static final int NO_POSITION = -1;
    private static final long NO_EXPIRY_TIME = -1L;

    private final MessageCache messageCache;
    private final MessageFW messageRO = new MessageFW();
    private final long tombstoneLifetimeMillis;
    private final Map<UnsafeBuffer, Entry> offsetsByKey;
    private final List<Entry> entries;

    private final List<DirectBuffer> tombstoneKeys = new ArrayList<>(100);
    private final LongArrayList tombstoneExpiryTimes = new LongArrayList(100, NO_EXPIRY_TIME);

    private final EntryIterator iterator = new EntryIterator();
    private final NoMessagesIterator noMessagesIterator = new NoMessagesIterator();
    private final UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final Entry candidate = new Entry(0L, NO_MESSAGE, NO_POSITION);

    private int compactFrom = Integer.MAX_VALUE;
    private long validFromOffset = 0L;
    private long validToOffset = 0L;

    public PartitionIndex(
        int initialCapacity,
        int tombstoneLifetimeMillis,
        MessageCache messageCache)
    {
        this.offsetsByKey = new HashMap<>(initialCapacity);
        entries = new ArrayList<Entry>(initialCapacity);
        this.messageCache = messageCache;
        this.tombstoneLifetimeMillis = tombstoneLifetimeMillis;
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
        Entry entry = null;

        // Only cache if there are no gaps in observed offsets and we have not yet observed this offset
        if (requestOffset <= validToOffset && messageOffset >= validToOffset)
        {
            validToOffset = Math.max(validToOffset,  nextFetchOffset);
            buffer.wrap(key, 0, key.capacity());
            entry = offsetsByKey.get(buffer);
            if (entry == null)
            {
                UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.capacity()]);
                keyCopy.putBytes(0,  key, 0, key.capacity());
                entry = new Entry(messageOffset, NO_MESSAGE, entries.size());
                offsetsByKey.put(keyCopy, entry);
                entries.add(entry);
                if (value == null)
                {
                    entry.message = TOMBSTONE_MESSAGE;
                    tombstoneKeys.add(key);
                    tombstoneExpiryTimes.add(System.currentTimeMillis() + tombstoneLifetimeMillis);
                }
                else
                {
                    entry.message = messageCache.put(timestamp, traceId, key, headers, value);
                }
            }
            else
            {
                compactFrom = Math.min(compactFrom, entry.position);
                entry.position = entries.size();
                entry.offset = messageOffset;
                entries.add(entry);
                if (value == null)
                {
                    entry.message = TOMBSTONE_MESSAGE;
                    tombstoneKeys.add(key);
                    tombstoneExpiryTimes.add(System.currentTimeMillis() + tombstoneLifetimeMillis);
                }
                else
                {
                    entry.message = messageCache.replace(entry.message, timestamp, traceId, key, headers, value);
                }
            }
        }
        else if (requestOffset <= validToOffset)
        {
            // cache the message if it was previously evicted due to lack of space
            buffer.wrap(key, 0, key.capacity());
            entry = offsetsByKey.get(buffer);
            if (messageCache.get(entry.message, messageRO) == null)
            {
                entry.message = messageCache.replace(entry.message, timestamp, traceId, key, headers, value);
            }
        }
    }

    public Iterator<Entry> entries(
        long requestOffset)
    {
        Iterator<Entry> result;
        int position = locate(requestOffset);
        if (position == -1)
        {
            long offset = Math.max(requestOffset, validToOffset);
            result = noMessagesIterator.reset(offset);
        }
        else
        {
            iterator.position = position;
            result = iterator;
        }
        return result;
    }

    public Entry getEntry(
        long requestOffset,
        OctetsFW key)
    {
        buffer.wrap(key.buffer(), key.offset(), key.sizeof());
        Entry result = offsetsByKey.get(buffer);
        if (result == null)
        {
            long offset = Math.max(requestOffset, validToOffset);
            result = noMessagesIterator.reset(offset).next();
        }
        return result;
    }

    public void setPartitionStartOffset(
        long firstOffset)
    {
        assert firstOffset >= validFromOffset;
        validFromOffset = validToOffset = firstOffset;
    }

    private void compact()
    {
        evictExpiredTombstones();
        if (compactFrom < entries.size())
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
            for (int i=entries.size() - 1; i > ceiling - 1; i--)
            {
                entries.remove(i);
            }
            compactFrom = Integer.MAX_VALUE;
        }
    }

    private void evictExpiredTombstones()
    {
        if (tombstoneKeys.size() > 0)
        {
            final long now = System.currentTimeMillis();
            int pos = 0;
            for (pos=0; pos < tombstoneKeys.size(); pos++)
            {
                if (now >= tombstoneExpiryTimes.getLong(pos))
                {
                    tombstoneExpiryTimes.setLong(pos, NO_EXPIRY_TIME);
                    DirectBuffer key = tombstoneKeys.set(pos,  null);
                    buffer.wrap(key, 0, key.capacity());
                    Entry entry = offsetsByKey.remove(buffer);
                    compactFrom = Math.min(entry.position, compactFrom);
                    entry.position = NO_POSITION;
                }
                else
                {
                    // expiry times are in increasing order
                    break;
                }
            }
            if (pos == tombstoneKeys.size())
            {
                tombstoneKeys.clear();
                tombstoneExpiryTimes.clear();
            }
            else if (pos > 0)
            {
                tombstoneKeys.removeIf(k -> k == null);
                tombstoneExpiryTimes.removeIf(t -> t == null);
            }
        }
    }

    private int locate(
        long offset)
    {
        compact();
        candidate.offset = offset;
        int result;
        if (offset >= validToOffset)
        {
            result = NO_POSITION;
        }
        else
        {
            result = Collections.binarySearch(entries, candidate);
            if (result < 0)
            {
                result = -result - 1;
            }
        }
        return result;
    }

    public final class EntryIterator implements Iterator<Entry>
    {
        private int position;

        @Override
        public boolean hasNext()
        {
            return position < entries.size();
        }

        @Override
        public Entry next()
        {
            // For efficiency reasons we don't guard for position < entries.size()
            return entries.get(position++);
        }
    }

    public final class NoMessagesIterator implements Iterator<Entry>
    {
        private Entry entry = new Entry(0L, NO_MESSAGE, NO_POSITION);
        private int remaining;

        NoMessagesIterator reset(long offset)
        {
            entry.offset = offset;
            remaining = 1;
            return this;
        }

        @Override
        public boolean hasNext()
        {
            return remaining > 0;
        }

        @Override
        public Entry next()
        {
            if (remaining-- > 0)
            {
                return entry;
            }
            else
            {
                 throw new NoSuchElementException();
            }
        }
    }

    public static final class Entry implements Comparable<Entry>
    {
        private long offset;
        private int  message;
        private int  position;

        private  Entry(
            long offset,
            int  message,
            int  position
            )
        {
            this.offset = offset;
            this.message = message;
            this.position = position;
        }

        public long offset()
        {
            return offset;
        }

        public int message()
        {
            return message;
        }

        @Override
        public int compareTo(
            Entry o)
        {
            return (int) (this.offset - o.offset);
        }

        @Override
        public String toString()
        {
            return String.format("Entry[%d, %d, %d]", offset, position, message);
        }
    }

}
