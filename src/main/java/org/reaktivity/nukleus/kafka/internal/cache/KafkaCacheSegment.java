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

import static java.nio.ByteBuffer.allocateDirect;

import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public abstract class KafkaCacheSegment implements Comparable<KafkaCacheSegment>
{
    public static final int POSITION_END_OF_MESSAGES = -1;
    public static final int POSITION_END_OF_SEGMENT = -2;
    public static final int OFFSET_LATEST = -1;

    private static final UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer(0, 0);

    protected final MutableDirectBuffer writeBuffer;
    protected final Path directory;
    protected final int logCapacity;
    protected final int indexCapacity;

    protected volatile KafkaCacheSegment.Data nextSegment;

    protected KafkaCacheSegment(
        MutableDirectBuffer writeBuffer,
        Path directory,
        int logCapacity,
        int indexCapacity)
    {
        this.writeBuffer = writeBuffer;
        this.directory = directory;
        this.logCapacity = logCapacity;
        this.indexCapacity = indexCapacity;
    }

    public int position(
        long nextOffset)
    {
        final DirectBuffer buffer = index();
        final long baseOffset = baseOffset();
        final int maxIndex = (buffer.capacity() >> 3) - 1;

        int lowIndex = 0;
        int highIndex = maxIndex;

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

        if (lowIndex <= highIndex || (lowIndex >= 0 && lowIndex <= maxIndex))
        {
            return (int)(buffer.getLong(lowIndex << 3) & 0xFFFF_FFFF);
        }
        else
        {
            return nextSegment != null ? POSITION_END_OF_SEGMENT : POSITION_END_OF_MESSAGES;
        }
    }

    public KafkaCacheEntryFW read(
        int position,
        KafkaCacheEntryFW entry)
    {
        assert position >= 0;
        final DirectBuffer buffer = log();
        return entry.tryWrap(buffer, position, buffer.capacity());
    }

    @Override
    public int compareTo(
        KafkaCacheSegment that)
    {
        return Long.compare(baseOffset(), that.baseOffset());
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

    @Override
    public String toString()
    {
        return String.format("%s %d", getClass().getSimpleName(), baseOffset());
    }

    public KafkaCacheSegment.Data nextSegment()
    {
        return nextSegment;
    }

    public KafkaCacheSegment.Data nextSegment(
        long nextOffset)
    {
        KafkaCacheSegment.Data nextSegment = this.nextSegment;
        if (nextSegment == null)
        {
            nextSegment = new KafkaCacheSegment.Data(writeBuffer, directory, nextOffset, logCapacity, indexCapacity);
            this.nextSegment = nextSegment;
        }
        return nextSegment;
    }

    public abstract long baseOffset();

    public long nextOffset()
    {
        return baseOffset();
    }

    public boolean log(
        DirectBuffer buffer,
        int index,
        int length)
    {
        return false;
    }

    public boolean index(
        DirectBuffer buffer,
        int index,
        int length)
    {
        return false;
    }

    public void lastOffset(
        long lastOffset)
    {
        throw new UnsupportedOperationException();
    }

    protected abstract DirectBuffer log();

    protected abstract DirectBuffer index();

    protected int logRemaining()
    {
        return logCapacity - log().capacity();
    }

    protected int indexRemaining()
    {
        return indexCapacity - index().capacity();
    }

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
            super(EMPTY_BUFFER, null, 0, 0);
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
        protected DirectBuffer log()
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
            Path directory,
            int maxLogCapacity,
            int maxIndexCapacity)
        {
            super(new UnsafeBuffer(allocateDirect(64 * 1024)), directory, maxLogCapacity, maxIndexCapacity); // TODO: configure
        }

        @Override
        public long baseOffset()
        {
            return -2L; // EARLIEST
        }

        @Override
        protected DirectBuffer log()
        {
            return EMPTY_BUFFER;
        }

        @Override
        protected DirectBuffer index()
        {
            return EMPTY_BUFFER;
        }

        @Override
        protected int logRemaining()
        {
            return 0;
        }

        @Override
        protected int indexRemaining()
        {
            return 0;
        }
    }

    public static final class Data extends KafkaCacheSegment
    {
        private final long baseOffset;
        private final KafkaCacheFile logFile;
        private final KafkaCacheFile indexFile;

        private long lastOffset;

        private Data(
            MutableDirectBuffer writeBuffer,
            Path directory,
            long baseOffset,
            int logCapacity,
            int indexCapacity)
        {
            super(writeBuffer, directory, logCapacity, indexCapacity);
            this.baseOffset = baseOffset;
            this.lastOffset = OFFSET_LATEST;
            this.logFile = new KafkaCacheFile(writeBuffer, directory, "log", baseOffset, logCapacity);
            this.indexFile = new KafkaCacheFile(writeBuffer, directory, "index", baseOffset, indexCapacity);
        }

        public void lastOffset(
            long lastOffset)
        {
            assert lastOffset > this.lastOffset;
            this.lastOffset = lastOffset;
        }

        @Override
        public long baseOffset()
        {
            return baseOffset;
        }

        @Override
        public long nextOffset()
        {
            return lastOffset == OFFSET_LATEST ? baseOffset : lastOffset + 1;
        }

        @Override
        public boolean log(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final boolean writable = logFile.readable().capacity() + length < logCapacity;
            if (writable)
            {
                logFile.write(buffer, index, length);
            }
            return writable;
        }

        @Override
        public boolean index(
            DirectBuffer buffer,
            int index,
            int length)
        {
            final boolean writable = indexFile.readable().capacity() + length <= indexCapacity;
            if (writable)
            {
                indexFile.write(buffer, index, length);
            }
            return writable;
        }

        @Override
        protected DirectBuffer index()
        {
            return indexFile.readable();
        }

        @Override
        protected DirectBuffer log()
        {
            return logFile.readable();
        }
    }
}
