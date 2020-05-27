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
import static java.util.Objects.requireNonNull;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.RETRY_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorRetryValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheIndexRecord.SIZEOF_INDEX_RECORD;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType.JSON_PATCH;
import static org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW.FIELD_OFFSET_DELTA_POSITION;
import static org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW.FIELD_OFFSET_DESCENDANT;
import static org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW.FIELD_OFFSET_FLAGS;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.zip.CRC32C;

import javax.json.JsonArray;
import javax.json.JsonPatch;
import javax.json.JsonReader;
import javax.json.JsonStructure;
import javax.json.JsonWriter;
import javax.json.spi.JsonProvider;

import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.io.DirectBufferInputStream;
import org.agrona.io.ExpandableDirectBufferOutputStream;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheDeltaFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public final class KafkaCachePartition
{
    private static final long NO_DIRTY_SINCE = -1L;
    private static final long NO_ANCESTOR_OFFSET = -1L;
    private static final long NO_DESCENDANT_OFFSET = -1L;
    private static final int NO_DELTA_POSITION = -1;

    private static final String FORMAT_PARTITION_DIRECTORY = "%s-%d";

    private static final int CACHE_ENTRY_FLAGS_DIRTY = 0x01;

    public static final int OFFSET_EARLIEST = KafkaOffsetType.EARLIEST.value();

    public static final int OFFSET_LATEST = KafkaOffsetType.LATEST.value();

    private final KafkaCacheEntryFW headEntryRO = new KafkaCacheEntryFW();
    private final KafkaCacheEntryFW logEntryRO = new KafkaCacheEntryFW();
    private final KafkaCacheDeltaFW deltaEntryRO = new KafkaCacheDeltaFW();

    private final MutableDirectBuffer entryInfo = new UnsafeBuffer(new byte[4 * Long.BYTES + 2 * Integer.BYTES]);
    private final MutableDirectBuffer valueInfo = new UnsafeBuffer(new byte[Integer.BYTES]);

    private final DirectBufferInputStream ancestorIn = new DirectBufferInputStream();
    private final DirectBufferInputStream headIn = new DirectBufferInputStream();
    private final MutableDirectBuffer diffBuffer = new ExpandableArrayBuffer();
    private final ExpandableDirectBufferOutputStream diffOut = new ExpandableDirectBufferOutputStream();

    private final Path location;
    private final KafkaCacheTopicConfig config;
    private final String cache;
    private final String topic;
    private final int id;
    private final MutableDirectBuffer appendBuf;
    private final IntFunction<long[]> sortSpaceRef;
    private final Node sentinel;
    private final CRC32C checksum;

    private long progress;

    private KafkaCacheEntryFW ancestorEntry;

    public KafkaCachePartition(
        Path location,
        KafkaCacheTopicConfig config,
        String cache,
        String topic,
        int id,
        int appendCapacity,
        IntFunction<long[]> sortSpaceRef)
    {
        this.location = createDirectories(location.resolve(String.format(FORMAT_PARTITION_DIRECTORY, topic, id)));
        this.config = config;
        this.cache = cache;
        this.topic = topic;
        this.id = id;
        this.appendBuf = new UnsafeBuffer(allocateDirect(appendCapacity));
        this.sortSpaceRef = sortSpaceRef;
        this.sentinel = new Node();
        this.checksum = new CRC32C();
        this.progress = OFFSET_EARLIEST;
    }

    public String cache()
    {
        return cache;
    }

    public String topic()
    {
        return topic;
    }

    public int id()
    {
        return id;
    }

    public Node sentinel()
    {
        return sentinel;
    }

    public Node head()
    {
        return sentinel.previous;
    }

    public long nextOffset(
        KafkaOffsetType defaultOffset)
    {
        final Node head = sentinel.previous;
        return head == sentinel ? defaultOffset.value() : head.segment().nextOffset();
    }

    public Node append(
        long offset)
    {
        assert offset >= progress;

        final Node head = sentinel.previous;

        KafkaCacheSegment segment = new KafkaCacheSegment(location, config, topic, id, offset, appendBuf, sortSpaceRef);
        Node node = new Node(segment);
        node.previous = head;
        node.next = sentinel;
        node.previous.next = node;
        node.next.previous = node;

        if (!head.sentinel())
        {
            final KafkaCacheSegment tail = head.segment.freeze();
            head.segment(tail);
        }

        return node;
    }

    public Node seekNotBefore(
        long offset)
    {
        Node node = sentinel.next;

        while (node != sentinel && node.segment.baseOffset() < offset)
        {
            node = node.next;
        }

        return node;
    }

    public Node seekNotAfter(
        long offset)
    {
        Node node = sentinel.previous;

        while (node != sentinel && node.segment.baseOffset() > offset)
        {
            node = node.previous;
        }

        return node;
    }

    public void newHeadIfNecessary(
        long offset)
    {
        if (head().sentinel())
        {
            append(offset);
        }
    }

    public Node newHeadIfNecessary(
        long offset,
        KafkaKeyFW key,
        int valueLength,
        int headersSizeMax)
    {
        Node head = sentinel.previous;

        if (head == sentinel)
        {
            head = append(offset);
        }
        else
        {
            final int logRequired = entryInfo.capacity() + key.sizeof() + valueInfo.capacity() +
                    Math.max(valueLength, 0) + headersSizeMax;
            final int hashKeyRequired = key.length() != -1 ? 1 : 0;
            final int hashHeaderRequiredMax = headersSizeMax >> 2;
            final int hashRequiredMax = (hashKeyRequired + hashHeaderRequiredMax) * SIZEOF_INDEX_RECORD;

            KafkaCacheSegment headSegment = head.segment;
            int logRemaining = headSegment.logFile().available();
            int indexRemaining = headSegment.indexFile().available();
            int hashRemaining = headSegment.hashFile().available();
            int nullsRemaining = headSegment.nullsFile().available();
            if (logRemaining < logRequired ||
                indexRemaining < SIZEOF_INDEX_RECORD ||
                hashRemaining < hashRequiredMax ||
                nullsRemaining < SIZEOF_INDEX_RECORD)
            {
                head = append(offset);
                headSegment = head.segment;
                logRemaining = headSegment.logFile().available();
                indexRemaining = headSegment.indexFile().available();
                hashRemaining = headSegment.hashFile().available();
                nullsRemaining = headSegment.nullsFile().available();
            }
            assert logRemaining >= logRequired;
            assert indexRemaining >= SIZEOF_INDEX_RECORD;
            assert hashRemaining >= hashRequiredMax;
            assert nullsRemaining >= SIZEOF_INDEX_RECORD;
        }

        return head;
    }

    public void writeEntry(
        long offset,
        long timestamp,
        KafkaKeyFW key,
        ArrayFW<KafkaHeaderFW> headers,
        OctetsFW value,
        KafkaCacheEntryFW ancestor,
        KafkaDeltaType deltaType)
    {
        final long keyHash = computeHash(key);
        writeEntryStart(offset, timestamp, key, keyHash, value != null ? value.sizeof() : -1, ancestor, deltaType);
        writeEntryContinue(value);
        writeEntryFinish(headers, deltaType);
    }

    public void writeEntryStart(
        long offset,
        long timestamp,
        KafkaKeyFW key,
        long keyHash,
        int valueLength,
        KafkaCacheEntryFW ancestor,
        KafkaDeltaType deltaType)
    {
        assert offset > this.progress : String.format("%d > %d", offset, this.progress);
        this.progress = offset;

        final Node head = sentinel.previous;
        assert head != sentinel;

        final KafkaCacheSegment segment = head.segment;
        assert segment != null;

        final KafkaCacheFile logFile = segment.logFile();
        final KafkaCacheFile deltaFile = segment.deltaFile();
        final KafkaCacheFile hashFile = segment.hashFile();
        final KafkaCacheFile keysFile = segment.keysFile();
        final KafkaCacheFile nullsFile = segment.nullsFile();

        logFile.mark();

        final long ancestorOffset = ancestor != null ? ancestor.offset$() : NO_ANCESTOR_OFFSET;
        final int deltaPosition = deltaType == JSON_PATCH &&
                                  ancestor != null && ancestor.valueLen() != -1 &&
                                  valueLength != -1
                    ? deltaFile.capacity()
                    : NO_DELTA_POSITION;

        assert deltaPosition == NO_DELTA_POSITION || ancestor != null;
        this.ancestorEntry = ancestor;

        entryInfo.putLong(0, progress);
        entryInfo.putLong(Long.BYTES, timestamp);
        entryInfo.putLong(2 * Long.BYTES, ancestorOffset);
        entryInfo.putLong(3 * Long.BYTES, NO_DESCENDANT_OFFSET);
        entryInfo.putInt(4 * Long.BYTES, 0x00);
        entryInfo.putInt(4 * Long.BYTES + Integer.BYTES, deltaPosition);

        logFile.appendBytes(entryInfo);
        logFile.appendBytes(key);
        logFile.appendInt(valueLength);

        final long hashEntry = keyHash << 32 | logFile.markValue();
        hashFile.appendLong(hashEntry);

        if (valueLength == -1)
        {
            final int timestampDelta = (int)((timestamp - segment.timestamp()) & 0xFFFF_FFFFL);
            final long nullsEntry = timestampDelta << 32 | logFile.markValue();
            nullsFile.appendLong(nullsEntry);
        }

        final int deltaBaseOffset = 0;
        final long keyEntry = keyHash << 32 | deltaBaseOffset;
        keysFile.appendLong(keyEntry);
    }

    public void writeEntryContinue(
        OctetsFW payload)
    {
        final Node head = sentinel.previous;
        assert head != sentinel;

        final KafkaCacheSegment headSegment = head.segment;
        assert headSegment != null;

        final KafkaCacheFile logFile = headSegment.logFile();

        final int logAvailable = logFile.available();
        final int logRequired = payload.sizeof();
        assert logAvailable >= logRequired;

        logFile.appendBytes(payload.buffer(), payload.offset(), payload.sizeof());
    }

    public void writeEntryFinish(
        ArrayFW<KafkaHeaderFW> headers,
        KafkaDeltaType deltaType)
    {
        final Node head = sentinel.previous;
        assert head != sentinel;

        final KafkaCacheSegment headSegment = head.segment;
        assert headSegment != null;

        final KafkaCacheFile logFile = headSegment.logFile();
        final KafkaCacheFile deltaFile = headSegment.deltaFile();
        final KafkaCacheFile hashFile = headSegment.hashFile();
        final KafkaCacheFile indexFile = headSegment.indexFile();

        final int logAvailable = logFile.available();
        final int logRequired = headers.sizeof();
        assert logAvailable >= logRequired : String.format("%s %d >= %d", headSegment, logAvailable, logRequired);

        logFile.appendBytes(headers);

        final long offsetDelta = (int)(progress - headSegment.baseOffset());
        final long indexEntry = (offsetDelta << 32) | logFile.markValue();

        if (!headers.isEmpty())
        {
            final DirectBuffer buffer = headers.buffer();
            final ByteBuffer byteBuffer = buffer.byteBuffer();
            assert byteBuffer != null;
            byteBuffer.clear();
            headers.forEach(h ->
            {
                final long hash = computeHash(h);
                final long hashEntry = (hash << 32) | logFile.markValue();
                hashFile.appendLong(hashEntry);
            });
        }

        assert indexFile.available() >= Long.BYTES;
        indexFile.appendLong(indexEntry);

        final KafkaCacheEntryFW headEntry = logFile.readBytes(logFile.markValue(), headEntryRO::wrap);

        if (deltaType == JSON_PATCH &&
            ancestorEntry != null && ancestorEntry.valueLen() != -1 &&
            headEntry.valueLen() != -1)
        {
            final OctetsFW ancestorValue = ancestorEntry.value();
            final OctetsFW headValue = headEntry.value();
            assert headEntry.offset$() == progress;

            final JsonProvider json = JsonProvider.provider();
            ancestorIn.wrap(ancestorValue.buffer(), ancestorValue.offset(), ancestorValue.sizeof());
            final JsonReader ancestorReader = json.createReader(ancestorIn);
            final JsonStructure ancestorJson = ancestorReader.read();
            ancestorReader.close();

            headIn.wrap(headValue.buffer(), headValue.offset(), headValue.sizeof());
            final JsonReader headReader = json.createReader(headIn);
            final JsonStructure headJson = headReader.read();
            headReader.close();

            final JsonPatch diff = json.createDiff(ancestorJson, headJson);
            final JsonArray diffJson = diff.toJsonArray();
            diffOut.wrap(diffBuffer, Integer.BYTES);
            final JsonWriter writer = json.createWriter(diffOut);
            writer.write(diffJson);
            writer.close();

            // TODO: signal delta.sizeof > head.sizeof via null delta, otherwise delta file can exceed log file

            final int deltaLength = diffOut.position();
            diffBuffer.putInt(0, deltaLength);
            deltaFile.appendBytes(diffBuffer, 0, Integer.BYTES + deltaLength);
        }

        headSegment.lastOffset(progress);
    }

    public long retainAt(
        KafkaCacheSegment segment)
    {
        return segment.timestamp() + config.segmentMillis;
    }

    public long deleteAt(
        KafkaCacheSegment segment,
        long retentionMillisMax)
    {
        return segment.timestamp() + Math.min(config.retentionMillis, retentionMillisMax);
    }

    public long compactAt(
        KafkaCacheSegment segment)
    {
        final long dirtySince = segment.dirtySince();

        long cleanableAt = segment.cleanableAt();
        if (cleanableAt == Long.MAX_VALUE && dirtySince != NO_DIRTY_SINCE)
        {
            final double cleanableDirtyRatio = segment.cleanableDirtyRatio();
            if (cleanableDirtyRatio >= config.minCleanableDirtyRatio)
            {
                final long now = System.currentTimeMillis();

                cleanableAt = Math.min(dirtySince + config.minCompactionLagMillis, now);
            }
            else if (cleanableDirtyRatio != 0.0 && config.maxCompactionLagMillis != Long.MAX_VALUE)
            {
                final long now = System.currentTimeMillis();

                cleanableAt = Math.min(dirtySince + config.maxCompactionLagMillis, now);
            }

            if (cleanableAt != Long.MAX_VALUE)
            {
                segment.cleanableAt(cleanableAt);
            }
        }

        return cleanableAt;
    }

    public KafkaCacheCleanupPolicy cleanupPolicy()
    {
        return config.cleanupPolicy;
    }

    public long computeKeyHash(
        KafkaKeyFW key)
    {
        return computeHash(key);
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s[%d]", cache, topic, id);
    }

    private long computeHash(
        Flyweight keyOrHeader)
    {
        // TODO: compute null key hash in advance
        final DirectBuffer buffer = keyOrHeader.buffer();
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        byteBuffer.clear();
        assert byteBuffer != null;
        checksum.reset();
        byteBuffer.position(keyOrHeader.offset());
        byteBuffer.limit(keyOrHeader.limit());
        checksum.update(byteBuffer);
        return checksum.getValue();
    }

    public final class Node
    {
        private volatile KafkaCacheSegment segment;
        private volatile KafkaCachePartition.Node previous;
        private volatile KafkaCachePartition.Node next;

        Node()
        {
            this.segment = null;
            this.previous = this;
            this.next = this;
        }

        Node(
            KafkaCacheSegment segment)
        {
            this.segment = requireNonNull(segment);
            this.previous = sentinel;
            this.next = sentinel;
        }

        public boolean sentinel()
        {
            return this == sentinel;
        }

        public Node previous()
        {
            return previous;
        }

        public Node next()
        {
            return next;
        }

        public KafkaCacheSegment segment()
        {
            return segment;
        }

        public Node seekAncestor(
            long baseOffset)
        {
            Node ancestorNode = this;

            while (!ancestorNode.sentinel() && ancestorNode.segment.baseOffset() > baseOffset)
            {
                ancestorNode = ancestorNode.previous;
            }

            return ancestorNode;
        }

        public void remove()
        {
            assert segment != null;
            segment.delete();
            segment.close();

            next.previous = previous;
            previous.next = next;
        }

        public void segment(
            KafkaCacheSegment segment)
        {
            assert segment != null;
            this.segment.close();
            this.segment = segment;
        }

        public void clean(
            long now)
        {
            assert next != sentinel; // not head segment

            if (segment.cleanableAt() <= now)
            {
                // TODO: use temporary files plus move to avoid corrupted log on restart
                segment.delete();

                final KafkaCacheSegment appender = new KafkaCacheSegment(segment, config, appendBuf, sortSpaceRef);
                final KafkaCacheFile logFile = segment.logFile();
                final KafkaCacheFile deltaFile = segment.deltaFile();

                for (int logPosition = 0; logPosition < logFile.capacity(); )
                {
                    final KafkaCacheEntryFW logEntry = logFile.readBytes(logPosition, logEntryRO::wrap);
                    if ((logEntry.flags() & CACHE_ENTRY_FLAGS_DIRTY) == 0)
                    {
                        final long logOffset = logEntry.offset$();
                        final KafkaKeyFW key = logEntry.key();
                        final ArrayFW<KafkaHeaderFW> headers = logEntry.headers();
                        final int deltaPosition = logEntry.deltaPosition();
                        final long keyHash = computeHash(key);

                        final long offsetDelta = (int)(logOffset - segment.baseOffset());
                        final long indexEntry = (offsetDelta << 32) | appender.logFile().capacity();
                        appender.indexFile().appendLong(indexEntry);

                        final long keyHashEntry = keyHash << 32 | appender.logFile().capacity();
                        appender.hashFile().appendLong(keyHashEntry);

                        headers.forEach(header ->
                        {
                            final long headerHash = computeHash(header);
                            final long headerHashEntry = headerHash << 32 | appender.logFile().capacity();
                            appender.hashFile().appendLong(headerHashEntry);
                        });

                        appender.logFile().appendBytes(logEntry);
                        if (deltaPosition != -1)
                        {
                            final int newLogEntryAt = appender.logFile().capacity() - logEntry.sizeof();
                            appender.logFile().writeInt(newLogEntryAt + FIELD_OFFSET_DELTA_POSITION, deltaFile.capacity());

                            final KafkaCacheDeltaFW deltaEntry = deltaFile.readBytes(deltaPosition, deltaEntryRO::wrap);
                            appender.deltaFile().appendBytes(deltaEntry);
                        }

                        // note: keys cleanup must also retain non-zero base offsets when spanning multiple segments
                        final int deltaBaseOffset = 0;
                        final long keyEntry = keyHash << 32 | deltaBaseOffset;
                        appender.keysFile().appendLong(keyEntry);

                        appender.lastOffset(logOffset);
                    }

                    logPosition = logEntry.limit();
                }

                segment.close();

                final KafkaCacheSegment frozen = appender.freeze();
                appender.close();

                if (frozen.logFile().empty())
                {
                    frozen.delete();
                    remove();
                }
                else
                {
                    segment(frozen);
                }
            }
        }

        public KafkaCacheEntryFW findAndMarkAncestor(
            KafkaKeyFW key,
            long hash,
            long descendantOffset,
            KafkaCacheEntryFW ancestorEntry)
        {
            KafkaCacheEntryFW ancestor = null;

            ancestor:
            if (key.length() != -1)
            {
                final KafkaCacheIndexFile hashFile = segment.hashFile();
                final KafkaCacheFile logFile = segment.logFile();
                long hashCursor = hashFile.last((int) hash);
                while (hashCursor != NEXT_SEGMENT && cursorValue(hashCursor) != cursorValue(RETRY_SEGMENT))
                {
                    final int position = cursorValue(hashCursor);
                    final KafkaCacheEntryFW cacheEntry = logFile.readBytes(position, ancestorEntry::wrap);
                    assert cacheEntry != null;
                    if (key.equals(cacheEntry.key()))
                    {
                        ancestor = cacheEntry;
                        markDescendantAndDirty(ancestor, descendantOffset);
                        break ancestor;
                    }

                    hashCursor = hashFile.lower((int) hash, hashCursor);
                }
                assert hashCursor == NEXT_SEGMENT || cursorRetryValue(hashCursor);
            }

            return ancestor;
        }

        private void markDescendantAndDirty(
            KafkaCacheEntryFW ancestor,
            long descendantOffset)
        {
            final KafkaCacheFile logFile = segment.logFile();
            logFile.writeLong(ancestor.offset() + FIELD_OFFSET_DESCENDANT, descendantOffset);
            logFile.writeInt(ancestor.offset() + FIELD_OFFSET_FLAGS, CACHE_ENTRY_FLAGS_DIRTY);
            segment.markDirtyBytes(ancestor.sizeof());
        }

        @Override
        public String toString()
        {
            Function<KafkaCacheSegment, String> baseOffset = s -> s != null ? Long.toString(s.baseOffset()) : "sentinel";
            return String.format("[%s] %s", getClass().getSimpleName(), baseOffset.apply(segment));
        }
    }

    private static Path createDirectories(
        Path directory)
    {
        try
        {
            Files.createDirectories(directory);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return directory;
    }
}
