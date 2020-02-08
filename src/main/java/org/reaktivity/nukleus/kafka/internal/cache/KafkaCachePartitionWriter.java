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

import static java.util.Collections.singleton;

import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public final class KafkaCachePartitionWriter
{
    private final MutableDirectBuffer entryInfo = new UnsafeBuffer(new byte[Long.BYTES + Long.BYTES]);
    private final MutableDirectBuffer valueInfo = new UnsafeBuffer(new byte[Integer.BYTES]);
    private final MutableDirectBuffer indexInfo = new UnsafeBuffer(new byte[Integer.BYTES + Integer.BYTES]);
    private final MutableDirectBuffer hashInfo = new UnsafeBuffer(new byte[Integer.BYTES + Integer.BYTES]);

    private final int partitionId;
    private final NavigableSet<KafkaCacheSegment> segments;
    private final CRC32C checksum;

    private KafkaCacheSegment entrySegment;
    private long offset;
    private int position;

    KafkaCachePartitionWriter(
        int partitionId,
        KafkaCacheSegment segment)
    {
        this.partitionId = partitionId;
        this.segments = new TreeSet<>(singleton(segment));
        this.entrySegment = segment;
        this.checksum = new CRC32C();
    }

    public int id()
    {
        return partitionId;
    }

    public void ensureWritable(
        long offset)
    {
        advance();

        if (entrySegment.baseOffset() < 0)
        {
            entrySegment = entrySegment.nextSegment(offset);
        }
        assert entrySegment.baseOffset() >= 0L;
    }

    public KafkaCachePartitionWriter advance()
    {
        KafkaCacheSegment nextSegment = entrySegment.nextSegment();
        while (nextSegment != null)
        {
            segments.add(nextSegment);
            entrySegment = nextSegment;
            nextSegment = entrySegment.nextSegment();
        }

        return this;
    }

    public long nextOffset()
    {
        return entrySegment.nextOffset();
    }

    public void writeEntry(
        long offset,
        long timestamp,
        KafkaKeyFW key,
        ArrayFW<KafkaHeaderFW> headers,
        OctetsFW value)
    {
        writeEntryStart(offset, timestamp, key, value.sizeof(), headers.sizeof());
        writeEntryContinue(value);
        writeEntryFinish(headers);
    }

    public void writeEntryStart(
        long offset,
        long timestamp,
        KafkaKeyFW key,
        int valueLength,
        int headerSizeMax)
    {
        final long nextOffset = entrySegment.nextOffset();
        assert offset >= 0 && offset >= nextOffset : String.format("%d >= 0 && %d >= %d", offset, offset, nextOffset);

        final int logRequired = entryInfo.capacity() + key.sizeof() + valueInfo.capacity() +
                Math.max(valueLength, 0) + headerSizeMax;
        final int indexRequired = indexInfo.capacity();
        final int hashKeyRequired = key.length() != -1 ? 1 : 0;
        final int hashHeaderRequiredMax = headerSizeMax >> 2;
        final int hashRequiredMax = (hashKeyRequired + hashHeaderRequiredMax) * hashInfo.capacity();

        int logRemaining = entrySegment.logRemaining();
        int indexRemaining = entrySegment.indexRemaining();
        int hashRemaining = entrySegment.hashRemaining();
        if (logRemaining < logRequired ||
            indexRemaining < indexRequired ||
            hashRemaining < hashRequiredMax)
        {
            entrySegment = entrySegment.nextSegment(offset);
            logRemaining = entrySegment.logRemaining();
            indexRemaining = entrySegment.indexRemaining();
            hashRemaining = entrySegment.hashRemaining();
        }
        assert logRemaining >= logRequired;
        assert indexRemaining >= indexRequired;
        assert hashRemaining >= hashRequiredMax;

        this.offset = offset;
        this.position = entrySegment.logPosition();

        entryInfo.putLong(0, offset);
        entryInfo.putLong(Long.BYTES, timestamp);
        valueInfo.putInt(0, valueLength);

        entrySegment.writeLog(entryInfo, 0, entryInfo.capacity());
        entrySegment.writeLog(key.buffer(), key.offset(), key.sizeof());
        entrySegment.writeLog(valueInfo, 0, valueInfo.capacity());

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
        hashInfo.putLong(0, (hash << 32) | position);
        entrySegment.writeHash(hashInfo, 0, hashInfo.capacity());
    }

    public void writeEntryContinue(
        OctetsFW payload)
    {
        final int logRemaining = entrySegment.logRemaining();
        final int logRequired = payload.sizeof();
        assert logRemaining >= logRequired;

        entrySegment.writeLog(payload.buffer(), payload.offset(), payload.sizeof());
    }

    public void writeEntryFinish(
        ArrayFW<KafkaHeaderFW> headers)
    {
        final int logRemaining = entrySegment.logRemaining();
        final int logRequired = headers.sizeof();
        assert logRemaining >= logRequired;

        entrySegment.writeLog(headers.buffer(), headers.offset(), headers.sizeof());

        final long offsetDelta = (int)(offset - entrySegment.baseOffset());
        indexInfo.putLong(0, (offsetDelta << 32) | position);

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
                hashInfo.putLong(0, (hash << 32) | position);
                entrySegment.writeHash(hashInfo, 0, hashInfo.capacity());
            });
        }

        final int indexRemaining = entrySegment.indexRemaining();
        final int indexRequired = indexInfo.capacity();
        assert indexRemaining >= indexRequired;

        entrySegment.writeIndex(indexInfo, 0, indexInfo.capacity());

        entrySegment.lastOffset(offset);
    }
}
