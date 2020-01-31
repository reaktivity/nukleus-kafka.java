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

    private final KafkaCacheSegment.Candidate candidate = new KafkaCacheSegment.Candidate();

    private final int partitionId;
    private final NavigableSet<KafkaCacheSegment> segments;

    private KafkaCacheSegment entrySegment;
    private long entryOffset;
    private int entryPosition;

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

    public KafkaCacheSegment seek(
        long offset)
    {
        KafkaCacheSegment nextSegment = entrySegment.nextSegment();
        while (nextSegment != null)
        {
            segments.add(nextSegment);
            entrySegment = nextSegment;
            nextSegment = entrySegment.nextSegment();
        }

        candidate.baseOffset(offset);
        return segments.floor(candidate);
    }

    public long nextOffset()
    {
        return entrySegment.nextOffset();
    }

    public void writeEntry(
        long entryOffset,
        long timestamp,
        KafkaKeyFW key,
        ArrayFW<KafkaHeaderFW> headers,
        OctetsFW payload)
    {
        writeEntryStart(entryOffset, timestamp, key);
        writeEntryContinue(payload);
        writeEntryFinish(headers);
    }

    public void writeEntryStart(
        long entryOffset,
        long timestamp,
        KafkaKeyFW key)
    {
        assert entryOffset >= 0 && entryOffset >= entrySegment.nextOffset();

        final int remaining = entrySegment.logRemaining();
        final int required = Long.BYTES + Long.BYTES + key.sizeof();
        if (remaining < required)
        {
            entrySegment = entrySegment.nextSegment(entryOffset);
            final int newRemaining = entrySegment.logRemaining();
            assert newRemaining >= required;
        }

        this.entryOffset = entryOffset;
        this.entryPosition = entrySegment.log().capacity();

        entryInfo.putLong(0, entryOffset);
        entryInfo.putLong(Long.BYTES, timestamp);

        entrySegment.log(entryInfo, 0, entryInfo.capacity());
        entrySegment.log(key.buffer(), key.offset(), key.sizeof());
    }

    public void writeEntryContinue(
        OctetsFW payload)
    {
        final int remaining = entrySegment.logRemaining();
        final int required = payload.sizeof();
        assert remaining >= required;

        entrySegment.log(payload.buffer(), payload.offset(), payload.sizeof());
    }

    public void writeEntryFinish(
        ArrayFW<KafkaHeaderFW> headers)
    {
        final int logRemaining = entrySegment.logRemaining();
        final int logRequired = headers.sizeof();
        assert logRemaining >= logRequired;

        entrySegment.log(headers.buffer(), headers.offset(), headers.sizeof());

        entryInfo.putLong(0, entryOffset);
        entryInfo.putLong(Long.BYTES, entryPosition);

        final int indexRemaining = entrySegment.indexRemaining();
        final int indexRequired = entryInfo.capacity();
        assert indexRemaining >= indexRequired;

        entrySegment.index(entryInfo, 0, entryInfo.capacity());

        entrySegment.nextOffset(entryOffset + 1);
    }
}
