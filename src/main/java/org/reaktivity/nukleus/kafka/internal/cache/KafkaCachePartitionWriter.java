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

import java.util.NavigableSet;
import java.util.TreeSet;

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

    private final int partitionId;
    private final NavigableSet<KafkaCacheSegment> segments;

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

        int logRemaining = entrySegment.logRemaining();
        int indexRemaining = entrySegment.indexRemaining();
        if (logRemaining < logRequired || indexRemaining < indexRequired)
        {
            entrySegment = entrySegment.nextSegment(offset);
            logRemaining = entrySegment.logRemaining();
            indexRemaining = entrySegment.indexRemaining();
        }
        assert logRemaining >= logRequired;
        assert indexRemaining >= indexRequired;

        this.offset = offset;
        this.position = entrySegment.log().capacity();

        entryInfo.putLong(0, offset);
        entryInfo.putLong(Long.BYTES, timestamp);
        valueInfo.putInt(0, valueLength);

        entrySegment.log(entryInfo, 0, entryInfo.capacity());
        entrySegment.log(key.buffer(), key.offset(), key.sizeof());
        entrySegment.log(valueInfo, 0, valueInfo.capacity());
    }

    public void writeEntryContinue(
        OctetsFW payload)
    {
        final int logRemaining = entrySegment.logRemaining();
        final int logRequired = payload.sizeof();
        assert logRemaining >= logRequired;

        entrySegment.log(payload.buffer(), payload.offset(), payload.sizeof());
    }

    public void writeEntryFinish(
        ArrayFW<KafkaHeaderFW> headers)
    {
        final int logRemaining = entrySegment.logRemaining();
        final int logRequired = headers.sizeof();
        assert logRemaining >= logRequired;

        entrySegment.log(headers.buffer(), headers.offset(), headers.sizeof());

        final long offsetDelta = (int)(offset - entrySegment.baseOffset());
        indexInfo.putLong(0, (offsetDelta << 32) | position);

        final int indexRemaining = entrySegment.indexRemaining();
        final int indexRequired = indexInfo.capacity();
        assert indexRemaining >= indexRequired;

        entrySegment.index(indexInfo, 0, indexInfo.capacity());

        entrySegment.lastOffset(offset);
    }
}
