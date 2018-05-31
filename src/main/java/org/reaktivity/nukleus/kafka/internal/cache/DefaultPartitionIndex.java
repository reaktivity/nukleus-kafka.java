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

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class DefaultPartitionIndex implements PartitionIndex
{
    private final NoMessageEntry noMessageEntry = new NoMessageEntry();
    private final NoMessageIterator noMessageIterator = new NoMessageIterator();

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
        // no-op
    }

    @Override
    public Iterator<Entry> entries(
        long requestOffset)
    {
        return noMessageIterator.offset(requestOffset);
    }

    @Override
    public void extendNextOffset(
        long requestOffset,
        long lastOffset)
    {
    }

    @Override
    public Entry getEntry(
        long requestOffset,
        OctetsFW key)
    {
        return noMessageEntry.offset(requestOffset);
    }

    @Override
    public long nextOffset()
    {
        return 0L;
    }

    private static final class NoMessageEntry implements Entry
    {
        private long offset;

        NoMessageEntry offset(long offset)
        {
            this.offset = offset;
            return this;
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public int message()
        {
            return NO_MESSAGE;
        }
    }

    private final class NoMessageIterator implements Iterator<Entry>
    {
        private final NoMessageEntry entry = new NoMessageEntry();
        private boolean hasNext;

        NoMessageIterator offset(long offset)
        {
            entry.offset(offset);
            hasNext = true;
            return this;
        }

        @Override
        public boolean hasNext()
        {
            return hasNext;
        }

        @Override
        public Entry next()
        {
            if (hasNext)
            {
                hasNext = false;
                return entry;
            }
            else
            {
                 throw new NoSuchElementException();
            }
        }
    }
}