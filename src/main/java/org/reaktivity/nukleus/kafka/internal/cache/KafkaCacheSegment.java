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
    protected final int hashCapacity;

    protected volatile KafkaCacheSegment.Data nextSegment;

    protected KafkaCacheSegment(
        MutableDirectBuffer writeBuffer,
        Path directory,
        int logCapacity,
        int indexCapacity,
        int hashCapacity)
    {
        this.writeBuffer = writeBuffer;
        this.directory = directory;
        this.logCapacity = logCapacity;
        this.indexCapacity = indexCapacity;
        this.hashCapacity = hashCapacity;
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
            nextSegment = new KafkaCacheSegment.Data(writeBuffer, directory, nextOffset,
                    logCapacity, indexCapacity, hashCapacity);
            this.nextSegment = nextSegment;
            freeze();
        }
        return nextSegment;
    }

    public abstract long baseOffset();

    public long nextOffset()
    {
        return baseOffset();
    }

    public int logPosition()
    {
        return 0;
    }

    public int logRemaining()
    {
        return 0;
    }

    public int indexRemaining()
    {
        return 0;
    }

    public int hashRemaining()
    {
        return 0;
    }

    public KafkaCacheEntryFW readLog(
        int position,
        KafkaCacheEntryFW entry)
    {
        return null;
    }

    public int readOffsetPosition(
        int index)
    {
        return -1;
    }

    public long readHashOffset(
        int hashIndex)
    {
        return -1;
    }

    public int findOffsetIndex(
        long nextOffset,
        int indexHint)
    {
        return -1;
    }

    public int findHashIndex(
        int hash,
        int indexHint)
    {
        return -1;
    }

    public boolean writeLog(
        DirectBuffer buffer,
        int index,
        int length)
    {
        return false;
    }

    public boolean writeIndex(
        DirectBuffer buffer,
        int index,
        int length)
    {
        return false;
    }

    public boolean writeHash(
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

    protected void freeze()
    {
        // ignore
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
            super(EMPTY_BUFFER, null, 0, 0, 0);
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
    }

    public static final class Sentinel extends KafkaCacheSegment
    {
        public Sentinel(
            Path directory,
            int maxLogCapacity,
            int maxIndexCapacity,
            int maxHashCapacity)
        {
            super(new UnsafeBuffer(allocateDirect(64 * 1024)), directory,
                    maxLogCapacity, maxIndexCapacity, maxHashCapacity); // TODO: configure
        }

        @Override
        public long baseOffset()
        {
            return -2L; // EARLIEST
        }
    }

    public static final class Data extends KafkaCacheSegment
    {
        private final long baseOffset;
        private final KafkaCacheLogFile logFile;
        private final KafkaCacheLogIndexFile indexFile;
        private KafkaCacheHashIndexFile hashFile;

        private long lastOffset;

        private Data(
            MutableDirectBuffer writeBuffer,
            Path directory,
            long baseOffset,
            int logCapacity,
            int indexCapacity,
            int hashCapacity)
        {
            super(writeBuffer, directory, logCapacity, indexCapacity, hashCapacity);
            this.baseOffset = baseOffset;
            this.lastOffset = OFFSET_LATEST;
            this.logFile = new KafkaCacheLogFile(writeBuffer, directory, baseOffset, logCapacity);
            this.indexFile = new KafkaCacheLogIndexFile(writeBuffer, directory, baseOffset, indexCapacity);
            this.hashFile = new KafkaCacheHashIndexFile(writeBuffer, directory, baseOffset, indexCapacity);
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
        public int logPosition()
        {
            return logFile.readableBuf.capacity();
        }

        @Override
        public int logRemaining()
        {
            return logFile.available();
        }

        @Override
        public int indexRemaining()
        {
            return indexFile.available();
        }

        @Override
        public int hashRemaining()
        {
            return hashFile.available();
        }

        @Override
        public KafkaCacheEntryFW readLog(
            int position,
            KafkaCacheEntryFW entry)
        {
            return logFile.read(position, entry);
        }

        @Override
        public int findHashIndex(
            int hash,
            int indexHint)
        {
            return hashFile.findHash(hash, indexHint);
        }

        @Override
        public long readHashOffset(
            int hashIndex)
        {
            return hashFile.offset(hashIndex);
        }

        @Override
        public int findOffsetIndex(
            long nextOffset,
            int indexHint)
        {
            return indexFile.findOffset(nextOffset, indexHint);
        }

        @Override
        public int readOffsetPosition(
            int offsetIndex)
        {
            return indexFile.position(offsetIndex);
        }

        @Override
        public boolean writeLog(
            DirectBuffer buffer,
            int index,
            int length)
        {
            return logFile.write(buffer, index, length);
        }

        @Override
        public boolean writeIndex(
            DirectBuffer buffer,
            int index,
            int length)
        {
            return indexFile.write(buffer, index, length);
        }

        @Override
        public boolean writeHash(
            DirectBuffer buffer,
            int index,
            int length)
        {
            return hashFile.write(buffer, index, length);
        }

        @Override
        protected void freeze()
        {
            logFile.freeze();
            indexFile.freeze();
            hashFile.freeze();
        }
    }
}
