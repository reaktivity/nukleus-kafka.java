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

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.OFFSET_EARLIEST;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheHeadSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSentinelSegment;
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
    private final KafkaCacheSentinelSegment sentinel;

    private KafkaCacheHeadSegment headSegment;

    KafkaCachePartitionWriter(
        int partitionId,
        KafkaCacheSentinelSegment sentinel)
    {
        this.partitionId = partitionId;
        this.sentinel = sentinel;
        this.headSegment = sentinel.headSegment();
    }

    public int id()
    {
        return partitionId;
    }

    public void ensureWritable(
        long offset)
    {
        headSegment = sentinel.headSegment(offset);
        assert headSegment.baseOffset() >= 0L;
    }

    public long nextOffset()
    {
        return headSegment != null ? headSegment.nextOffset() : OFFSET_EARLIEST;
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
        final KafkaCacheHeadSegment headSegment = nextSegmentIfNecessary(offset, key, valueLength, headerSizeMax);

        final long nextOffset = headSegment.nextOffset();
        assert offset >= 0 && offset >= nextOffset : String.format("%d >= 0 && %d >= %d", offset, offset, nextOffset);

        headSegment.writeEntryStart(offset, timestamp, key, valueLength);
    }

    public void writeEntryContinue(
        OctetsFW payload)
    {
        headSegment.writeEntryContinue(payload);
    }

    public void writeEntryFinish(
        ArrayFW<KafkaHeaderFW> headers)
    {
        headSegment.writeEntryFinish(headers);
    }

    private KafkaCacheHeadSegment nextSegmentIfNecessary(
        long offset,
        KafkaKeyFW key,
        int valueLength,
        int headerSizeMax)
    {
        if (headSegment == null)
        {
            headSegment = sentinel.headSegment(offset);
        }
        else
        {
            final int logRequired = entryInfo.capacity() + key.sizeof() + valueInfo.capacity() +
                    Math.max(valueLength, 0) + headerSizeMax;
            final int indexRequired = indexInfo.capacity();
            final int hashKeyRequired = key.length() != -1 ? 1 : 0;
            final int hashHeaderRequiredMax = headerSizeMax >> 2;
            final int hashRequiredMax = (hashKeyRequired + hashHeaderRequiredMax) * hashInfo.capacity();

            int logRemaining = headSegment.logFile.available();
            int indexRemaining = headSegment.indexFile.available();
            int hashRemaining = headSegment.hashFile.available();
            if (logRemaining < logRequired ||
                indexRemaining < indexRequired ||
                hashRemaining < hashRequiredMax)
            {
                headSegment = headSegment.nextSegment(offset);
                logRemaining = headSegment.logFile.available();
                indexRemaining = headSegment.indexFile.available();
                hashRemaining = headSegment.hashFile.available();
            }
            assert logRemaining >= logRequired;
            assert indexRemaining >= indexRequired;
            assert hashRemaining >= hashRequiredMax;
        }

        return headSegment;
    }
}
