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
import java.util.Objects;

import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class CompactedPartitionIndex implements PartitionIndex
{
    private static final int NO_MESSAGE = MessageCache.NO_MESSAGE;
    private static final int NO_POSITION = -1;
    private static final long NO_EXPIRY_TIME = -1L;

    private final MessageCache messageCache;
    private final MessageFW messageRO = new MessageFW();
    private final long tombstoneLifetimeMillis;
    private final Map<UnsafeBuffer, EntryImpl> offsetsByKey;
    private final List<EntryImpl> entries;

    private final List<DirectBuffer> tombstoneKeys = new ArrayList<>(100);
    private final LongArrayList tombstoneExpiryTimes = new LongArrayList(100, NO_EXPIRY_TIME);

    private final EntryIterator iterator = new EntryIterator();
    private final NoMessagesIterator noMessagesIterator = new NoMessagesIterator();
    private final UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final EntryImpl candidate = new EntryImpl(0L, NO_MESSAGE, NO_POSITION);

    private int compactFrom = Integer.MAX_VALUE;
    private long validToOffset = 0L;

    public CompactedPartitionIndex(
        int initialCapacity,
        int tombstoneLifetimeMillis,
        MessageCache messageCache)
    {
        this.offsetsByKey = new HashMap<>(initialCapacity);
        this.entries = new ArrayList<EntryImpl>(initialCapacity);
        this.messageCache = messageCache;
        this.tombstoneLifetimeMillis = tombstoneLifetimeMillis;
    }

    @Override
    public void add(
        long requestOffset,
        long messageStartOffset,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value)
    {
        buffer.wrap(key, 0, key.capacity());
        EntryImpl entry = offsetsByKey.get(buffer);

        // Only cache if there are no gaps in observed offsets and we have not yet observed this offset
        if (requestOffset <= validToOffset && messageStartOffset >= validToOffset)
        {
            validToOffset = Math.max(validToOffset,  messageStartOffset + 1);
            if (entry == null)
            {
                UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.capacity()]);
                keyCopy.putBytes(0,  key, 0, key.capacity());
                entry = new EntryImpl(messageStartOffset, NO_MESSAGE, entries.size());
                offsetsByKey.put(keyCopy, entry);
            }
            else
            {
                compactFrom = Math.min(compactFrom, entry.position);
                entry.position = entries.size();
                entry.offset = messageStartOffset;
            }
            entries.add(entry);
            cacheMessage(entry, timestamp, traceId, key, headers, value);
        }
        else if (requestOffset > validToOffset)
        {
            // Out of order add, we can still cache the offset and message for the key
            // as long as it does not affect existing entries
            if (entry == null)
            {
                UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.capacity()]);
                keyCopy.putBytes(0,  key, 0, key.capacity());
                entry = new EntryImpl(messageStartOffset, NO_MESSAGE, entries.size());
                offsetsByKey.put(keyCopy, entry);
                cacheMessage(entry, timestamp, traceId, key, headers, value);
            }
        }
        else if (messageCache.get(entry.message, messageRO) == null)
        {
            // historical message which was previously evicted due to lack of space, re-cache it
            entry.message = messageCache.replace(entry.message, timestamp, traceId, key, headers, value);
        }
    }

    @Override
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

    @Override
    public void extendNextOffset(
        long requestOffset,
        long lastOffset)
    {
        if (requestOffset < validToOffset)
        {
            validToOffset = Math.max(lastOffset,  validToOffset);
        }
    }

    @Override
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

    @Override
    public long nextOffset()
    {
        return validToOffset;
    }

    private void cacheMessage(
        EntryImpl entry,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value)
    {
        if (value == null)
        {
            tombstoneKeys.add(key);
            tombstoneExpiryTimes.add(System.currentTimeMillis() + tombstoneLifetimeMillis);
        }
        if (entry.message == NO_MESSAGE)
        {
            entry.message = messageCache.put(timestamp, traceId, key, headers, value);
        }
        else
        {
            entry.message = messageCache.replace(entry.message, timestamp, traceId, key, headers, value);
        }
    }

    private void compact()
    {
        evictExpiredTombstones();
        if (compactFrom < entries.size())
        {
            int ceiling = NO_POSITION;
            for (int i=0; i < entries.size(); i++)
            {
                EntryImpl entry = entries.get(i);
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
                    EntryImpl entry = offsetsByKey.remove(buffer);
                    messageCache.release(entry.message);
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
                tombstoneKeys.removeIf(Objects::isNull);
                tombstoneExpiryTimes.removeIf(Objects::isNull);
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
        private EntryImpl entry = new EntryImpl(0L, NO_MESSAGE, NO_POSITION);
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

    public static final class EntryImpl implements Comparable<EntryImpl>, Entry
    {
        private long offset;
        private int  message;
        private int  position;

        private  EntryImpl(
            long offset,
            int  message,
            int  position)
        {
            this.offset = offset;
            this.message = message;
            this.position = position;
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public int message()
        {
            return message;
        }

        @Override
        public int compareTo(
            EntryImpl o)
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
