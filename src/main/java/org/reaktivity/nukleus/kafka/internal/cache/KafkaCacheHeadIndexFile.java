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

import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.index;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.record;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.value;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.RETRY_SEGMENT;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheHeadSegment;

public abstract class KafkaCacheHeadIndexFile extends KafkaCacheHeadFile
{
    protected KafkaCacheHeadIndexFile(
        KafkaCacheHeadSegment segment,
        String extension,
        MutableDirectBuffer writeBuffer,
        int writeCapacity)
    {
        super(segment, extension, writeBuffer, writeCapacity);
    }

    protected long seekKey(
        int key)
    {
        // assumes sorted by key
        final DirectBuffer buffer = readableBuf;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (readCapacity >> 3) - 1;

        int lowIndex = 0;
        int highIndex = lastIndex;

        while (lowIndex <= highIndex)
        {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final long entry = buffer.getLong(midIndex << 3);
            final int entryKey = (int)(entry >>> 32);

            if (entryKey < key)
            {
                lowIndex = midIndex + 1;
            }
            else if (entryKey > key)
            {
                highIndex = midIndex - 1;
            }
            else
            {
                return entry;
            }
        }

        assert lowIndex >= 0;
        if (lowIndex <= lastIndex)
        {
            final long entry = buffer.getLong(lowIndex << 3);
            return record(lowIndex, value(entry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected long seekValue(
        int key,
        int value)
    {
        // assumes sorted by value, repeated keys
        final DirectBuffer buffer = readableBuf;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (readCapacity >> 3) - 1;

        int lowIndex = 0;
        int highIndex = lastIndex;

        while (lowIndex <= highIndex)
        {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final long entry = buffer.getLong(midIndex << 3);
            final int entryValue = (int)(entry & 0x7FFF_FFFF);

            if (entryValue < value)
            {
                lowIndex = midIndex + 1;
            }
            else if (entryValue > value)
            {
                highIndex = midIndex - 1;
            }
            else
            {
                final int entryKey = (int)(entry >>> 32);
                if (entryKey == key)
                {
                    lowIndex = midIndex + 1;
                }
                else
                {
                    break;
                }
            }
        }

        assert lowIndex >= 0;
        if (lowIndex <= lastIndex)
        {
            final long entry = buffer.getLong(lowIndex << 3);
            return record(lowIndex, value(entry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected long scanKey(
        int key,
        long record)
    {
        // assumes sorted by key, record from seekKey
        final int index = index(record);
        final int value = value(record);
        assert index >= 0;

        final DirectBuffer buffer = readableBuf;
        final int capacity = readCapacity;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (capacity >> 3) - 1;

        int currentIndex = index;
        while (currentIndex <= lastIndex)
        {
            final long entry = buffer.getLong(currentIndex << 3);
            final int entryKey = (int)(entry >>> 32);
            final int entryValue = (int)(entry & 0x7FFF_FFFF);
            if (entryKey != key || entryValue == value)
            {
                break;
            }
            currentIndex++;
        }

        if (currentIndex <= lastIndex)
        {
            final long entry = buffer.getLong(currentIndex << 3);
            return record(currentIndex, value(entry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected long scanValue(
        int key,
        long record)
    {
        // assumes sorted by value, repeated keys, record from seekValue
        final int index = KafkaCacheCursorRecord.index(record);
        final int value = KafkaCacheCursorRecord.value(record);
        assert index >= 0;

        final DirectBuffer buffer = readableBuf;
        final int capacity = readCapacity;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (capacity >> 3) - 1;

        int currentIndex = index;
        while (currentIndex <= lastIndex)
        {
            final long entry = buffer.getLong(currentIndex << 3);
            final int entryKey = (int)(entry >>> 32);
            final int entryValue = (int)(entry & 0x7FFF_FFFF);
            if (entryKey == key && entryValue >= value)
            {
                break;
            }
            currentIndex++;
        }

        if (currentIndex <= lastIndex)
        {
            final long entry = buffer.getLong(currentIndex << 3);
            return record(currentIndex, value(entry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected long scanIndex(
        long record)
    {
        final int index = KafkaCacheCursorRecord.index(record);
        assert index >= 0;

        final DirectBuffer buffer = readableBuf;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (readCapacity >> 3) - 1;

        if (index <= lastIndex)
        {
            final long entry = buffer.getLong(index << 3);
            return record(index, value(entry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected void sort(
        Path unsortedFile,
        Path sortingFile,
        Path sortedFile)
    {
        try
        {
            Files.copy(unsortedFile, sortingFile);

            try (FileChannel channel = FileChannel.open(sortingFile, READ, WRITE))
            {
                final ByteBuffer mapped = channel.map(MapMode.READ_WRITE, 0, channel.size());
                final MutableDirectBuffer buffer = new UnsafeBuffer(mapped);
                final int capacity = buffer.capacity();

                // TODO: better O(N) sort algorithm
                final int maxIndex = capacity - Long.BYTES;
                for (int index = 0, priorIndex = index; index < maxIndex; priorIndex = index += Long.BYTES)
                {
                    final long candidate = buffer.getLong(index + Long.BYTES);
                    while (priorIndex >= 0 &&
                           Long.compareUnsigned(candidate, buffer.getLong(priorIndex)) < 0)
                    {
                        buffer.putLong(priorIndex + Long.BYTES, buffer.getLong(priorIndex));
                        priorIndex -= Long.BYTES;
                    }
                    buffer.putLong(priorIndex + Long.BYTES, candidate);
                }

                IoUtil.unmap(mapped);
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            Files.move(sortingFile, sortedFile);
            Files.delete(unsortedFile);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
