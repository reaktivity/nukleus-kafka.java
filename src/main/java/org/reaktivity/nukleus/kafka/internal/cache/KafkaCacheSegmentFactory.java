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
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.RETRY_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.value;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
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
    public static final int POSITION_UNSET = -1;

    public static final int OFFSET_LATEST = -1;
    public static final int OFFSET_EARLIEST = -2;

    public static final String CACHE_EXTENSION_LOG = "log";
    public static final String CACHE_EXTENSION_LOG_INDEX = "index";
    public static final String CACHE_EXTENSION_HASH_SCAN = "hscan";
    public static final String CACHE_EXTENSION_HASH_SCAN_SORTING = String.format("%s.sorting", CACHE_EXTENSION_HASH_SCAN);
    public static final String CACHE_EXTENSION_HASH_INDEX = "hindex";
    public static final String CACHE_EXTENSION_KEYS_SCAN = "kscan";
    public static final String CACHE_EXTENSION_KEYS_SCAN_SORTING = String.format("%s.sorting", CACHE_EXTENSION_KEYS_SCAN);
    public static final String CACHE_EXTENSION_KEYS_INDEX = "kindex";

    private final KafkaCacheEntryFW entryRO = new KafkaCacheEntryFW();

    private final MutableDirectBuffer entryInfo = new UnsafeBuffer(new byte[Long.BYTES + Long.BYTES + Long.BYTES]);
    private final MutableDirectBuffer valueInfo = new UnsafeBuffer(new byte[Integer.BYTES]);
    private final MutableDirectBuffer indexInfo = new UnsafeBuffer(new byte[Integer.BYTES + Integer.BYTES]);
    private final MutableDirectBuffer hashInfo = new UnsafeBuffer(new byte[Integer.BYTES + Integer.BYTES]);
    private final MutableDirectBuffer keysInfo = new UnsafeBuffer(new byte[Integer.BYTES + Integer.BYTES]);

    private final int maxLogCapacity;
    private final int maxIndexCapacity;
    private final int maxHashCapacity;
    private final int maxKeysCapacity;
    private final MutableDirectBuffer writeBuffer;
    private final CRC32C checksum;

    KafkaCacheSegmentFactory(
        KafkaConfiguration config)
    {
        this.maxLogCapacity = config.cacheSegmentLogBytes();
        this.maxIndexCapacity = config.cacheSegmentIndexBytes();
        this.maxHashCapacity = config.cacheSegmentHashBytes();
        this.maxKeysCapacity = config.cacheSegmentKeysBytes();
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

            final KafkaCacheSegment frozenSegment = freezeSegment(nextOffset);

            assert nextSegment == frozenSegment.nextSegment;

            return nextSegment.headSegment();
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

        protected KafkaCacheSegment freezeSegment(
            long nextOffset)
        {
            this.nextSegment = new KafkaCacheHeadSegment(this, directory, nextOffset);
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
        final KafkaCacheHeadKeysFile keysFile;

        private long headOffset;
        private int headPosition;

        private KafkaCacheHeadSegment(
            KafkaCacheSegment previousSegment,
            Path directory,
            long baseOffset)
        {
            this(previousSegment, directory, baseOffset, null);
        }

        private KafkaCacheHeadSegment(
            KafkaCacheSegment previousSegment,
            Path directory,
            long baseOffset,
            KafkaCacheTailKeysFile previousKeys)
        {
            super(previousSegment, directory);
            this.baseOffset = baseOffset;
            this.headOffset = OFFSET_LATEST;
            this.logFile = new KafkaCacheHeadLogFile(this, writeBuffer, maxLogCapacity);
            this.indexFile = new KafkaCacheHeadLogIndexFile(this, writeBuffer, maxIndexCapacity);
            this.hashFile = new KafkaCacheHeadHashFile(this, writeBuffer, maxHashCapacity);
            this.keysFile = new KafkaCacheHeadKeysFile(this, writeBuffer, maxKeysCapacity, previousKeys);
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
        protected KafkaCacheTailSegment freezeSegment(
            long nextOffset)
        {
            logFile.freeze();
            indexFile.freeze();
            hashFile.freeze();

            hashFile.toHashIndex();
            keysFile.toKeysIndex();

            final KafkaCacheTailSegment tailSegment = new KafkaCacheTailSegment(previousSegment, directory, baseOffset);
            final KafkaCacheTailKeysFile tailKeys = new KafkaCacheTailKeysFile(tailSegment, keysFile.previousKeys);
            tailSegment.nextSegment = new KafkaCacheHeadSegment(this, directory, nextOffset, tailKeys);
            previousSegment.nextSegment = tailSegment;
            this.nextSegment = tailSegment.nextSegment;
            return tailSegment;
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

            long ancestor = -1L;
            ancestor:
            if (key.length() != -1)
            {
                long hashRecord = hashFile.scanHash((int) hash, 0);
                while (hashRecord != NEXT_SEGMENT && hashRecord != RETRY_SEGMENT)
                {
                    final int position = value(hashRecord);
                    final KafkaCacheEntryFW entry = logFile.read(position, entryRO);
                    assert entry != null;
                    if (key.equals(entry.key()))
                    {
                        ancestor = entry.offset$();
                        break ancestor;
                    }

                    final long nextHashRecord = hashFile.scanHash((int) hash, hashRecord);
                    if (nextHashRecord == NEXT_SEGMENT || nextHashRecord == RETRY_SEGMENT)
                    {
                        break;
                    }

                    hashRecord = nextHashRecord;
                }
                assert hashRecord == NEXT_SEGMENT || hashRecord == RETRY_SEGMENT;

                KafkaCacheTailKeysFile previousKeys = keysFile.previousKeys;
                while (previousKeys != null)
                {
                    long keyRecord = previousKeys.seekKey((int) hash, Integer.MIN_VALUE);
                    while (keyRecord != NEXT_SEGMENT && keyRecord != RETRY_SEGMENT)
                    {
                        final long ancestorBaseOffset = previousKeys.baseOffset(keyRecord);

                        KafkaCacheSegment ancestorSegment = this;
                        while (ancestorSegment != null && ancestorSegment.baseOffset() > ancestorBaseOffset)
                        {
                            ancestorSegment = ancestorSegment.previousSegment;
                        }

                        if (ancestorSegment != null && ancestorSegment.baseOffset() == ancestorBaseOffset)
                        {
                            long ancestorRecord = ancestorSegment.seekHash((int) hash, 0);
                            while (ancestorRecord != NEXT_SEGMENT && ancestorRecord != RETRY_SEGMENT)
                            {
                                final int position = value(ancestorRecord);
                                final KafkaCacheEntryFW entry = ancestorSegment.readLog(position, entryRO);
                                assert entry != null;
                                if (key.equals(entry.key()))
                                {
                                    ancestor = entry.offset$();
                                    break ancestor;
                                }

                                final long nextAncestorRecord = ancestorSegment.scanHash((int) hash, ancestorRecord);
                                if (nextAncestorRecord < 0)
                                {
                                    break;
                                }

                                ancestorRecord = nextAncestorRecord;
                            }
                        }

                        final long nextKeyRecord = previousKeys.scanKey((int) hash, keyRecord);
                        if (nextKeyRecord < 0)
                        {
                            break;
                        }

                        keyRecord = nextKeyRecord;
                    }

                    previousKeys = previousKeys.previousKeys;
                }
            }

            entryInfo.putLong(0, headOffset);
            entryInfo.putLong(Long.BYTES, timestamp);
            entryInfo.putLong(Long.BYTES + Long.BYTES, ancestor);
            valueInfo.putInt(0, valueLength);

            logFile.write(entryInfo, 0, entryInfo.capacity());
            logFile.write(key.buffer(), key.offset(), key.sizeof());
            logFile.write(valueInfo, 0, valueInfo.capacity());

            final long hashEntry = hash << 32 | headPosition;
            hashInfo.putLong(0, hashEntry);
            hashFile.write(hashInfo, 0, hashInfo.capacity());

            final int deltaBaseOffset = 0;
            final long keyEntry = hash << 32 | deltaBaseOffset;
            keysInfo.putLong(0, keyEntry);
            keysFile.write(keysInfo, 0, keysInfo.capacity());
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
