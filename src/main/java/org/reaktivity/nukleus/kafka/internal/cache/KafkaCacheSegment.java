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

import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public abstract class KafkaCacheSegment implements Comparable<KafkaCacheSegment>
{
    public static final int END_OF_MESSAGES = -1;
    public static final int END_OF_SEGMENT = -2;

    private static final UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer(0, 0);

    protected final Path directory;
    protected final int maxCapacity;

    protected volatile KafkaCacheSegment nextSegment;

    protected KafkaCacheSegment(
        Path directory,
        int maxCapacity)
    {
        this.directory = directory;
        this.maxCapacity = maxCapacity;
    }

    public int position(
        long nextOffset)
    {
        final DirectBuffer buffer = index();
        final long baseOffset = baseOffset();

        int lowIndex = 0;
        int highIndex = (buffer.capacity() >> 3) - 1;

        while (lowIndex <= highIndex)
        {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final long relativeEntry = buffer.getLong(midIndex << 3);
            final int relativeOffset = (int)(relativeEntry >>> 32);
            final long absoluteOffset = baseOffset + relativeOffset;

            if (absoluteOffset < nextOffset)
            {
                lowIndex = midIndex + 1;
            }
            else if (absoluteOffset > nextOffset)
            {
                highIndex = midIndex - 1;
            }
            else
            {
                return (int)(relativeEntry & 0xFFFF_FFFF);
            }
        }

        // TODO: END_OF_MESSAGES vs END_OF_SEGMENT
        return lowIndex <= highIndex ? (int)(buffer.getLong(lowIndex << 3) & 0xFFFF_FFFF) : END_OF_MESSAGES;
    }

    public KafkaCacheEntryFW read(
        int position,
        KafkaCacheEntryFW entry)
    {
        assert position >= 0;
        final DirectBuffer buffer = messages();
        return entry.tryWrap(buffer, position, buffer.capacity());
    }

    @Override
    public int compareTo(
        KafkaCacheSegment that)
    {
        return (int)(baseOffset() - that.baseOffset());
    }

    @Override
    public int hashCode()
    {
        return Long.hashCode(baseOffset());
    }

    @Override
    public boolean equals(
        Object obj)
    {
        return this == obj ||
                (obj instanceof KafkaCacheSegment &&
                 equalTo((KafkaCacheSegment) obj));
    }

    public KafkaCacheSegment nextSegment()
    {
        return nextSegment;
    }

    public KafkaCacheSegment nextSegment(
        int nextOffset)
    {
        KafkaCacheSegment nextSegment = this.nextSegment;
        if (nextSegment == null)
        {
            this.nextSegment = nextSegment = new KafkaCacheSegment.Data(directory, nextOffset, maxCapacity);
        }
        return nextSegment;
    }

    public abstract long baseOffset();

    protected abstract DirectBuffer messages();

    protected abstract DirectBuffer index();

    private boolean equalTo(
        KafkaCacheSegment that)
    {
        return baseOffset() == that.baseOffset();
    }

    public static final class Candidate extends KafkaCacheSegment
    {
        private long baseOffset;

        public Candidate()
        {
            super(null, 0);
        }

        public void baseOffset(
                long baseOffset)
        {
            this.baseOffset = baseOffset;
        }

        @Override
        public long baseOffset()
        {
            return baseOffset;
        }

        @Override
        protected DirectBuffer messages()
        {
            return EMPTY_BUFFER;
        }

        @Override
        protected DirectBuffer index()
        {
            return EMPTY_BUFFER;
        }
    }

    public static final class Sentinel extends KafkaCacheSegment
    {
        public Sentinel(
            Path directory)
        {
            super(directory, 0);
        }

        @Override
        public long baseOffset()
        {
            return -2L; // EARLIEST
        }

        @Override
        protected DirectBuffer messages()
        {
            return EMPTY_BUFFER;
        }

        @Override
        protected DirectBuffer index()
        {
            return EMPTY_BUFFER;
        }
    }

    private static final class Data extends KafkaCacheSegment
    {
        private final long baseOffset;
        private final KafkaCacheFile messages;
        private final KafkaCacheFile index;

        private Data(
            Path directory,
            long baseOffset,
            int capacity)
        {
            super(directory, capacity);
            this.baseOffset = baseOffset;
            this.messages = new KafkaCacheFile(directory, "log", baseOffset, capacity);
            this.index = new KafkaCacheFile(directory, "index", baseOffset, capacity);
        }

        @Override
        public long baseOffset()
        {
            return baseOffset;
        }

        @Override
        protected DirectBuffer index()
        {
            return index.readable();
        }

        @Override
        protected DirectBuffer messages()
        {
            return messages.readable();
        }
    }
}
