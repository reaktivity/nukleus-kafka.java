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

import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.EMPTY_BYTE_ARRAY;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.LongSupplier;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class CompactedPartitionIndex implements PartitionIndex
{
    private static final int NO_MESSAGE = MessageCache.NO_MESSAGE;
    private static final long NO_OFFSET = -1L;
    private static final int NO_POSITION = -1;
    private static final long NO_EXPIRY_TIME = -1L;

    static final int MAX_INVALID_ENTRIES = 10000;

    private final boolean proactive;
    private final MessageCache messageCache;
    private final LongSupplier cacheHits;
    private final LongSupplier cacheMisses;
    private final MessageFW messageRO = new MessageFW();
    private final HeadersFW headersRO = new HeadersFW();
    private final long tombstoneLifetimeMillis;
    private final Map<UnsafeBuffer, EntryImpl> entriesByKey;
    private final List<EntryImpl> entries;

    private final List<DirectBuffer> tombstoneKeys = new ArrayList<>(100);
    private final LongArrayList tombstoneExpiryTimes = new LongArrayList(100, NO_EXPIRY_TIME);

    private final EntryIterator iterator = new EntryIterator();
    private final NoMessagesIterator noMessagesIterator = new NoMessagesIterator();
    private final EntryImpl noMessageEntry = new EntryImpl(0L, NO_MESSAGE, NO_POSITION);
    private final UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final UnsafeBuffer buffer2 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final EntryImpl candidate = new EntryImpl(0L, NO_MESSAGE, NO_POSITION);

    private int compactFrom = Integer.MAX_VALUE;
    private int invalidEntries;
    private long validToOffset = 0L;

    public CompactedPartitionIndex(
        boolean proactive,
        int initialCapacity,
        int tombstoneLifetimeMillis,
        MessageCache messageCache,
        LongSupplier cacheHits,
        LongSupplier cacheMisses)
    {
        this.proactive = proactive;
        this.entriesByKey = new HashMap<>(initialCapacity);
        this.entries = new ArrayList<EntryImpl>(initialCapacity);
        this.messageCache = messageCache;
        this.cacheHits = cacheHits;
        this.cacheMisses = cacheMisses;
        this.tombstoneLifetimeMillis = tombstoneLifetimeMillis;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<UnsafeBuffer, EntryImpl> entry : entriesByKey.entrySet())
        {
            UnsafeBuffer key = entry.getKey();
            EntryImpl value = entry.getValue();
            byte[] keyArray = new byte[key.capacity()];
            key.getBytes(0, keyArray);
            sb.append(BitUtil.toHex(keyArray));
            sb.append("=");
            sb.append(value);
            sb.append(",");
        }
        sb.setLength(sb.length() - 1);
        sb.append("}");
        return String.format("[entries = %s, entriesByKey = %s]", entries, sb.toString());
    }

    @Override
    public void add(
        long requestOffset,
        long messageStartOffset,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value,
        boolean cacheIfNew)
    {
        if (invalidEntries > MAX_INVALID_ENTRIES)
        {
            compact();
            invalidEntries = 0;
        }
        buffer.wrap(key, 0, key.capacity());
        EntryImpl entry = entriesByKey.get(buffer);

        // Only cache if there are no gaps in observed offsets and we have not yet observed this offset
        if ((requestOffset <= validToOffset || entries.isEmpty()) && messageStartOffset >= validToOffset)
        {
            validToOffset = messageStartOffset + 1;
            if (entry == null)
            {
                UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.capacity()]);
                keyCopy.putBytes(0,  key, 0, key.capacity());
                entry = new EntryImpl(messageStartOffset, NO_MESSAGE, entries.size());
                entriesByKey.put(keyCopy, entry);
            }
            else
            {
                compactFrom = Math.min(compactFrom, entry.position());
                invalidEntries++;
                entry.setPosition(entries.size());
                entry.offset = messageStartOffset;

                if (entry.getAndSetIsTombstone(false))
                {
                    cancelTombstoneExpiry(buffer);
                }
            }
            entries.add(entry);
            if (value == null)
            {
                MutableDirectBuffer keyCopy = new UnsafeBuffer(new byte[key.capacity()]);
                keyCopy.putBytes(0,  key, 0, key.capacity());
                tombstoneKeys.add(keyCopy);
                tombstoneExpiryTimes.add(timestamp + tombstoneLifetimeMillis);
                entry.getAndSetIsTombstone(true);
            }
            if (cacheIfNew)
            {
                cacheMessage(entry, timestamp, traceId, key, headers, value);
            }
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
                entriesByKey.put(keyCopy, entry);
                if (cacheIfNew)
                {
                    cacheMessage(entry, timestamp, traceId, key, headers, value);
                }
            }
        }
        else if (entry != null && entry.offset == messageStartOffset && messageCache.get(entry.message, messageRO) == null)
        {
            // Always attempt to cache historical messages
            entry.message = messageCache.replace(entry.message, timestamp, traceId, key, headers, value);
        }
    }

    @Override
    public Iterator<Entry> entries(
        long requestOffset,
        ListFW<KafkaHeaderFW> headerConditions)
    {
        Iterator<Entry> result;
        int position = locate(requestOffset);
        if (position == NO_POSITION)
        {
            long offset = Math.max(requestOffset, validToOffset);
            result = noMessagesIterator.reset(offset);
        }
        else
        {
            result = iterator.reset(headerConditions, position);
        }
        return result;
    }

    @Override
    public void extendNextOffset(
        long requestOffset,
        long lastOffset)
    {
        if (requestOffset <= validToOffset)
        {
            validToOffset = Math.max(lastOffset,  validToOffset);
        }
    }

    @Override
    public Entry getEntry(
        OctetsFW key)
    {
        buffer.wrap(key.buffer(), key.offset(), key.sizeof());
        Entry result = entriesByKey.get(buffer);
        if (result != null)
        {
            MessageFW message = messageCache.get(result.messageHandle(), messageRO);
            if (message != null)
            {
                cacheHits.getAsLong();
            }
            else
            {
                cacheMisses.getAsLong();
            }
        }
        return result;
    }

    @Override
    public long getOffset(
        OctetsFW key)
    {
        buffer.wrap(key.buffer(), key.offset(), key.sizeof());
        Entry entry = entriesByKey.get(buffer);
        return entry == null ? NO_OFFSET : entry.offset();
    }

    @Override
    public long nextOffset()
    {
        return validToOffset;
    }

    @Override
    public void startOffset(
        long startOffset)
    {
        final long earliestOffset = earliestOffset();

        if (earliestOffset != NO_OFFSET && earliestOffset < startOffset)
        {
            for (Iterator<Map.Entry<UnsafeBuffer, EntryImpl>> iter = entriesByKey.entrySet().iterator(); iter.hasNext(); )
            {
                final Map.Entry<UnsafeBuffer, EntryImpl> entry = iter.next();
                final EntryImpl value = entry.getValue();
                if (value.offset < startOffset)
                {
                    if (value.message != NO_MESSAGE)
                    {
                        messageCache.release(value.message);
                        value.message = NO_MESSAGE;
                    }

                    value.position = NO_POSITION;
                    iter.remove();
                }
            }

            compact(0, startOffset);
        }
    }

    int numberOfEntries()
    {
        return entries.size();
    }

    private void cacheMessage(
        EntryImpl entry,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value)
    {
        if (entry.message == NO_MESSAGE)
        {
            entry.message = messageCache.put(timestamp, traceId, key, headers, value);
        }
        else
        {
            entry.message = messageCache.replace(entry.message, timestamp, traceId, key, headers, value);
        }
    }

    private void cancelTombstoneExpiry(
        UnsafeBuffer key)
    {
        int pos = 0;
        for (pos=0; pos < tombstoneKeys.size(); pos++)
        {
            DirectBuffer candidate = tombstoneKeys.get(pos);
            buffer2.wrap(candidate, 0, candidate.capacity());
            if (key.equals(buffer2))
            {
                 tombstoneKeys.remove(pos);
                 tombstoneExpiryTimes.remove(pos);
                 break;
            }
        }
    }

    private void compact()
    {
        evictExpiredTombstones(); // mutates compactFrom

        if (compactFrom != Integer.MAX_VALUE)
        {
            compact(0, Long.MAX_VALUE);
        }
        compactFrom = Integer.MAX_VALUE;
    }

    private void compact(
        int startPosition,
        long messageOffsetLimit)
    {
        evictExpiredTombstones();

        int invalidFrom = NO_POSITION;
        for (int i=startPosition; i < entries.size(); i++)
        {
            EntryImpl entry = entries.get(i);

            if (entry.offset >= messageOffsetLimit)
            {
                break;
            }

            if (entry.position() != i)
            {
                if (invalidFrom == NO_POSITION)
                {
                    invalidFrom = i;
                }
            }
            else if (invalidFrom != NO_POSITION)
            {
                entry.setPosition(invalidFrom);
                entries.set(invalidFrom, entry);
                invalidFrom++;
            }
        }

        if (invalidFrom != NO_POSITION)
        {
            for (int i=entries.size() - 1; i > invalidFrom - 1; i--)
            {
                entries.remove(i);
            }
        }
    }

    private long earliestOffset()
    {
        compact();      // TODO keep track a validFromOffset
        return entries.isEmpty() ? NO_OFFSET : entries.get(0).offset;
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
                    tombstoneExpiryTimes.set(pos, null);
                    DirectBuffer key = tombstoneKeys.set(pos, null);

                    buffer.wrap(key, 0, key.capacity());
                    EntryImpl entry = entriesByKey.remove(buffer);

                    if (entry != null)
                    {
                        if (entry.message != NO_MESSAGE)
                        {
                            messageCache.release(entry.message);
                        }

                        compactFrom = Math.min(entry.position(), compactFrom);
                        entry.position = NO_POSITION;
                    }
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

            assert tombstoneKeys.size() == tombstoneExpiryTimes.size();
        }
    }

    private int locate(
        long offset)
    {
        compact();
        candidate.offset = offset;
        int result;
        if (proactive && !entries.isEmpty() && offset < earliestOffset())
        {
            result = 0;         // catch up to the latest offset
        }
        else if (offset >= validToOffset)
        {
            result = NO_POSITION;
        }
        else
        {
            result = Collections.binarySearch(entries, candidate);
            if (result < 0)
            {
                result = -result - 1;

                if (result >= entries.size())
                {
                    result = NO_POSITION;
                }
            }
        }
        return result;
    }

    final class EntryIterator implements Iterator<Entry>
    {
        private final MessageFW messageRO = new MessageFW();

        private int position;
        private boolean hasNext;
        private ListFW<KafkaHeaderFW> headerConditions;

        @Override
        public boolean hasNext()
        {
            return hasNext;
        }

        @Override
        public Entry next()
        {
            EntryImpl entry = null;
            while (position < entries.size())
            {
                entry = entries.get(position);
                MessageFW message = messageCache.get(entry.messageHandle(), messageRO);
                if (message == null)
                {
                    hasNext = false;
                    cacheMisses.getAsLong();
                    break;
                }
                else if (headersRO.wrap(message.headers()).matches(headerConditions))
                {
                    cacheHits.getAsLong();
                    break;
                }
                else
                {
                    entry = null;
                }
                position++;
            }
            if (entry == null)
            {
                entry = noMessageEntry;
                entry.offset = nextOffset();
                hasNext = false;
            }
            position++;
            return entry;
        }

        Iterator<Entry> reset(
            ListFW<KafkaHeaderFW> headerConditions,
            int position)
        {
            this.headerConditions = headerConditions;
            this.position = position;
            this.hasNext = true;
            return this;
        }
    }

    final class NoMessagesIterator implements Iterator<Entry>
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

    static final class EntryImpl implements Comparable<EntryImpl>, Entry
    {
        private static final int IS_TOMBSTONE_MASK = 0x80000000;
        private static final int POSITION_MASK = ~IS_TOMBSTONE_MASK;

        private long offset;
        private int  message;
        private int  position;

         EntryImpl(
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
        public int messageHandle()
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
            return String.format("Entry[offset=%d, position=%d, %b, %d]", offset, position(), isTombstone(), message);
        }

        int position()
        {
            return position & POSITION_MASK;
        }

        void setPosition(int newPosition)
        {
            assert newPosition >= 0;
            position = (position & IS_TOMBSTONE_MASK) | newPosition;
        }

        boolean isTombstone()
        {
            return (position & IS_TOMBSTONE_MASK) == IS_TOMBSTONE_MASK;
        }

        boolean getAndSetIsTombstone(
            boolean isTombstone)
        {
            boolean priorValue = isTombstone();
            if (isTombstone)
            {
                position |= IS_TOMBSTONE_MASK;
            }
            else
            {
                position &= POSITION_MASK;
            }
            return priorValue;
        }
    }

}
