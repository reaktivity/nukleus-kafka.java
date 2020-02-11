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
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public final class KafkaCacheSegmentFactory
{
    public static final long RETRY_SEGMENT = Integer.MAX_VALUE - 1;
    public static final long NEXT_SEGMENT = Long.MAX_VALUE - 1;

    public static final int POSITION_UNSET = -1;

    public static final int OFFSET_LATEST = -1;
    public static final int OFFSET_EARLIEST = -2;

    public static final String CACHE_EXTENSION_LOG = "log";
    public static final String CACHE_EXTENSION_LOG_INDEX = "index";
    public static final String CACHE_EXTENSION_HASH_SCAN = "hscan";
    public static final String CACHE_EXTENSION_HASH_SCAN_SORTING = String.format("%s.sorting", CACHE_EXTENSION_HASH_SCAN);
    public static final String CACHE_EXTENSION_HASH_INDEX = "hindex";

    private final MutableDirectBuffer entryInfo = new UnsafeBuffer(new byte[Long.BYTES + Long.BYTES]);
    private final MutableDirectBuffer valueInfo = new UnsafeBuffer(new byte[Integer.BYTES]);
    private final MutableDirectBuffer indexInfo = new UnsafeBuffer(new byte[Integer.BYTES + Integer.BYTES]);
    private final MutableDirectBuffer hashInfo = new UnsafeBuffer(new byte[Integer.BYTES + Integer.BYTES]);

    private final int maxLogCapacity;
    private final int maxIndexCapacity;
    private final int maxHashCapacity;
    private final MutableDirectBuffer writeBuffer;
    private final CRC32C checksum;

    KafkaCacheSegmentFactory(
        KafkaConfiguration config)
    {
        this.maxLogCapacity = config.cacheSegmentLogBytes();
        this.maxIndexCapacity = config.cacheSegmentIndexBytes();
        this.maxHashCapacity = config.cacheSegmentHashBytes();
        this.writeBuffer = new UnsafeBuffer(allocateDirect(64 * 1024)); // TODO: configure
        this.checksum = new CRC32C();
    }

    KafkaCacheSentinelSegment newSentinel(
        Path directory)
    {
        return new KafkaCacheSentinelSegment(directory);
    }

    KafkaCacheCandidateSegment newCandidate()
    {
        return new KafkaCacheCandidateSegment();
    }

    public abstract class KafkaCacheSegment implements Comparable<KafkaCacheSegment>
    {
        protected final KafkaCacheSegment previousSegment;
        protected final Path directory;

        protected volatile KafkaCacheSegment nextSegment;

        protected KafkaCacheSegment(
            Path directory)
        {
            this.previousSegment = this;
            this.directory = directory;
        }

        protected KafkaCacheSegment(
            KafkaCacheSegment previousSegment,
            Path directory)
        {
            this.previousSegment = previousSegment;
            this.directory = directory;
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

        public KafkaCacheSegment nextSegment()
        {
            return nextSegment;
        }

        public KafkaCacheHeadSegment nextSegment(
            long nextOffset)
        {
            assert nextSegment == null;

            final KafkaCacheSegment frozenSegment = freezeSegment();
            final KafkaCacheHeadSegment nextSegment = new KafkaCacheHeadSegment(frozenSegment, directory, nextOffset);

            frozenSegment.nextSegment = nextSegment;
            previousSegment.nextSegment = frozenSegment;

            this.nextSegment = nextSegment;

            return nextSegment;
        }

        protected KafkaCacheHeadSegment headSegment()
        {
            KafkaCacheHeadSegment headSegment = null;

            if (nextSegment != null)
            {
                headSegment = nextSegment.headSegment();
            }

            return headSegment;
        }

        public abstract long baseOffset();

        public long seekOffset(
            long nextOffset)
        {
            return NEXT_SEGMENT;
        }

        public long scanOffset(
            long record)
        {
            return NEXT_SEGMENT;
        }

        public long seekHash(
            int hash,
            int position)
        {
            return NEXT_SEGMENT;
        }

        public long scanHash(
            int hash,
            long record)
        {
            return NEXT_SEGMENT;
        }

        public KafkaCacheEntryFW readLog(
            int position,
            KafkaCacheEntryFW entry)
        {
            return null;
        }

        protected KafkaCacheSegment freezeSegment()
        {
            return this;
        }

        protected final Path cacheFile(
            String extension)
        {
            return directory.resolve(String.format("%016x.%s", baseOffset(), extension));
        }

        private boolean equalTo(
            KafkaCacheSegment that)
        {
            return baseOffset() == that.baseOffset();
        }
    }

    public final class KafkaCacheCandidateSegment extends KafkaCacheSegment
    {
        private long baseOffset;

        KafkaCacheCandidateSegment()
        {
            super(null);
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

    final class KafkaCacheSentinelSegment extends KafkaCacheSegment
    {
        private KafkaCacheSentinelSegment(
            Path directory)
        {
            super(directory);
        }

        public KafkaCacheHeadSegment headSegment(
            long nextOffset)
        {
            return nextSegment != null ? nextSegment.headSegment() : nextSegment(nextOffset);
        }

        @Override
        public long baseOffset()
        {
            return OFFSET_EARLIEST;
        }
    }

    final class KafkaCacheHeadSegment extends KafkaCacheSegment
    {
        private final long baseOffset;
        final KafkaCacheHeadLogFile logFile;
        final KafkaCacheHeadLogIndexFile indexFile;
        final KafkaCacheHeadHashFile hashFile;

        private long headOffset;
        private int headPosition;

        private KafkaCacheHeadSegment(
            KafkaCacheSegment previousSegment,
            Path directory,
            long baseOffset)
        {
            super(previousSegment, directory);
            this.baseOffset = baseOffset;
            this.headOffset = OFFSET_LATEST;
            this.logFile = new KafkaCacheHeadLogFile(this, writeBuffer, maxLogCapacity);
            this.indexFile = new KafkaCacheHeadLogIndexFile(this, writeBuffer, maxIndexCapacity);
            this.hashFile = new KafkaCacheHeadHashFile(this, writeBuffer, maxHashCapacity);
        }

        @Override
        public long baseOffset()
        {
            return baseOffset;
        }

        public long nextOffset()
        {
            return headOffset == OFFSET_LATEST ? baseOffset : headOffset + 1;
        }

        @Override
        public long seekOffset(
            long nextOffset)
        {
            return indexFile.seekOffset(nextOffset);
        }

        @Override
        public long scanOffset(
            long record)
        {
            return indexFile.scanOffset(record);
        }

        @Override
        public long seekHash(
            int hash,
            int position)
        {
            return hashFile.seekHash(hash, position);
        }

        @Override
        public long scanHash(
            int hash,
            long record)
        {
            return hashFile.scanHash(hash, record);
        }

        @Override
        public KafkaCacheEntryFW readLog(
            int position,
            KafkaCacheEntryFW entry)
        {
            return logFile.read(position, entry);
        }

        protected KafkaCacheHeadSegment headSegment()
        {
            return this;
        }

        @Override
        protected KafkaCacheTailSegment freezeSegment()
        {
            logFile.freeze();
            indexFile.freeze();
            hashFile.freeze();

            sortHashScanAsHashIndex();

            return new KafkaCacheTailSegment(previousSegment, directory, baseOffset);
        }

        private void sortHashScanAsHashIndex()
        {
            try
            {
                final Path hashScanFile = cacheFile(CACHE_EXTENSION_HASH_SCAN);
                final Path hashScanSortingFile = cacheFile(CACHE_EXTENSION_HASH_SCAN_SORTING);
                final Path hashIndexFile = cacheFile(CACHE_EXTENSION_HASH_INDEX);

                Files.copy(hashScanFile, hashScanSortingFile);

                try (FileChannel channel = FileChannel.open(hashScanSortingFile, READ, WRITE))
                {
                    final ByteBuffer mapped = channel.map(MapMode.READ_WRITE, 0, channel.size());
                    final MutableDirectBuffer buffer = new UnsafeBuffer(mapped);

                    // TODO: better O(N) sort algorithm
                    int maxIndex = (buffer.capacity() >> 3) - 1;
                    for (int index = 0, priorIndex = index; index < maxIndex; priorIndex = ++index)
                    {
                        final long candidate = buffer.getLong((index + 1) << 3);
                        while (priorIndex >= 0 &&
                               Long.compareUnsigned(candidate, buffer.getLong(priorIndex << 3)) < 0)
                        {
                            buffer.putLong((priorIndex + 1) << 3, buffer.getLong(priorIndex << 3));
                            priorIndex--;
                        }
                        buffer.putLong((priorIndex + 1) << 3, candidate);
                    }

                    IoUtil.unmap(mapped);
                }
                catch (IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }

                Files.move(hashScanSortingFile, hashIndexFile);
                Files.delete(hashScanFile);
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        void writeEntry(
            long offset,
            long timestamp,
            KafkaKeyFW key,
            ArrayFW<KafkaHeaderFW> headers,
            OctetsFW value)
        {
            final int valueLength = value != null ? value.sizeof() : -1;
            writeEntryStart(offset, timestamp, key, valueLength);
            if (value != null)
            {
                writeEntryContinue(value);
            }
            writeEntryFinish(headers);
        }

        void writeEntryStart(
            long offset,
            long timestamp,
            KafkaKeyFW key,
            int valueLength)
        {
            assert offset > this.headOffset;
            this.headOffset = offset;
            this.headPosition = logFile.readCapacity;

            entryInfo.putLong(0, headOffset);
            entryInfo.putLong(Long.BYTES, timestamp);
            valueInfo.putInt(0, valueLength);

            logFile.write(entryInfo, 0, entryInfo.capacity());
            logFile.write(key.buffer(), key.offset(), key.sizeof());
            logFile.write(valueInfo, 0, valueInfo.capacity());

            // TODO: compute null key hash in advance
            final DirectBuffer buffer = key.buffer();
            final ByteBuffer byteBuffer = buffer.byteBuffer();
            byteBuffer.clear();
            assert byteBuffer != null;
            checksum.reset();
            byteBuffer.position(key.offset());
            byteBuffer.limit(key.limit());
            checksum.update(byteBuffer);
            final long hash = checksum.getValue();
            final long hashShifted31 = hash << 31;
            final long hashShifted32 = hashShifted31 << 1;
            final long hashEntry = hashShifted32 | headPosition;
            hashInfo.putLong(0, hashEntry);
            hashFile.write(hashInfo, 0, hashInfo.capacity());
        }


        void writeEntryContinue(
            OctetsFW payload)
        {
            final int logRemaining = logFile.writeCapacity - logFile.readCapacity;
            final int logRequired = payload.sizeof();
            assert logRemaining >= logRequired;

            logFile.write(payload.buffer(), payload.offset(), payload.sizeof());
        }

        void writeEntryFinish(
            ArrayFW<KafkaHeaderFW> headers)
        {
            final int logRemaining = logFile.writeCapacity - logFile.readCapacity;
            final int logRequired = headers.sizeof();
            assert logRemaining >= logRequired;

            logFile.write(headers.buffer(), headers.offset(), headers.sizeof());

            final long offsetDelta = (int)(headOffset - baseOffset());
            indexInfo.putLong(0, (offsetDelta << 32) | headPosition);

            if (!headers.isEmpty())
            {
                final DirectBuffer buffer = headers.buffer();
                final ByteBuffer byteBuffer = buffer.byteBuffer();
                assert byteBuffer != null;
                byteBuffer.clear();
                headers.forEach(h ->
                {
                    checksum.reset();
                    byteBuffer.position(h.offset());
                    byteBuffer.limit(h.limit());
                    checksum.update(byteBuffer);
                    final long hash = checksum.getValue();
                    hashInfo.putLong(0, (hash << 32) | headPosition);
                    hashFile.write(hashInfo, 0, hashInfo.capacity());
                });
            }

            final int indexRemaining = indexFile.writeCapacity - indexFile.readCapacity;
            final int indexRequired = indexInfo.capacity();
            assert indexRemaining >= indexRequired;

            indexFile.write(indexInfo, 0, indexInfo.capacity());
        }
    }

    final class KafkaCacheTailSegment extends KafkaCacheSegment
    {
        private final long baseOffset;
        final KafkaCacheTailLogFile logFile;
        final KafkaCacheTailLogIndexFile indexFile;
        final KafkaCacheTailHashFile hashFile;

        private KafkaCacheTailSegment(
            KafkaCacheSegment previousSegment,
            Path directory,
            long baseOffset)
        {
            super(previousSegment, directory);
            this.baseOffset = baseOffset;
            this.logFile = new KafkaCacheTailLogFile(this);
            this.indexFile = new KafkaCacheTailLogIndexFile(this);
            this.hashFile = new KafkaCacheTailHashFile(this);
        }

        @Override
        public long baseOffset()
        {
            return baseOffset;
        }

        @Override
        public long seekOffset(
            long nextOffset)
        {
            return indexFile.seekOffset(nextOffset);
        }

        @Override
        public long scanOffset(
            long record)
        {
            return indexFile.scanOffset(record);
        }

        @Override
        public long seekHash(
            int hash,
            int position)
        {
            return hashFile.seekHash(hash, position);
        }

        @Override
        public long scanHash(
            int hash,
            long record)
        {
            return hashFile.scanHash(hash, record);
        }

        @Override
        public KafkaCacheEntryFW readLog(
            int position,
            KafkaCacheEntryFW entry)
        {
            return logFile.read(position, entry);
        }
    }
}
