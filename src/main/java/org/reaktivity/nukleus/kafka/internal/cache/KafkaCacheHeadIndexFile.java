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

import static java.lang.Integer.compareUnsigned;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.RETRY_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursor;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorIndex;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheIndexRecord.indexKey;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheIndexRecord.indexValue;

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
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSegment;

public abstract class KafkaCacheHeadIndexFile extends KafkaCacheHeadFile
{
    protected KafkaCacheHeadIndexFile(
        KafkaCacheSegment segment,
        String extension,
        MutableDirectBuffer writeBuffer,
        int writeCapacity)
    {
        super(segment, extension, writeBuffer, writeCapacity);
    }

    protected Int2IntHashMap toMap()
    {
        final Int2IntHashMap map = new Int2IntHashMap(-1);
        for (int index = 0; index < readCapacity; index += Long.BYTES)
        {
            final long indexEntry = readableBuf.getLong(index);
            map.put(indexKey(indexEntry), indexValue(indexEntry));
        }
        return map;
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
            final long indexEntry = buffer.getLong(midIndex << 3);
            final int indexKey = indexKey(indexEntry);
            final int comparison = compareUnsigned(indexKey, key);

            if (comparison < 0)
            {
                lowIndex = midIndex + 1;
            }
            else if (comparison > 0)
            {
                highIndex = midIndex - 1;
            }
            else
            {
                break;
            }
        }

        assert lowIndex >= 0;
        if (lowIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(lowIndex << 3);
            return cursor(lowIndex, indexValue(indexEntry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected long seekValue(
        int key,
        int value)
    {
        // assumes sorted by (unsigned) value, repeated keys
        final DirectBuffer buffer = readableBuf;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (readCapacity >> 3) - 1;

        int lowIndex = 0;
        int highIndex = lastIndex;

        while (lowIndex <= highIndex)
        {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final long indexEntry = buffer.getLong(midIndex << 3);
            final int indexValue = indexValue(indexEntry);
            final int comparison = compareUnsigned(indexValue, value);

            if (comparison < 0)
            {
                lowIndex = midIndex + 1;
            }
            else if (comparison > 0)
            {
                highIndex = midIndex - 1;
            }
            else
            {
                final int indexKey = indexKey(indexEntry);
                if (indexKey != key)
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
            final long indexEntry = buffer.getLong(lowIndex << 3);
            return cursor(lowIndex, indexValue(indexEntry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected long scanValue(
        int key,
        long cursor)
    {
        // assumes sorted by (unsigned) value, repeated non-negative keys
        final int cursorIndex = cursorIndex(cursor);
        final int cursorValue = cursorValue(cursor);
        assert cursorIndex >= 0;

        final DirectBuffer buffer = readableBuf;
        final int capacity = readCapacity;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (capacity >> 3) - 1;

        int currentIndex = cursorIndex;
        while (currentIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(currentIndex << 3);
            final int indexKey = indexKey(indexEntry);
            final int indexValue = indexValue(indexEntry);

            if (indexKey == key && compareUnsigned(indexValue, cursorValue) >= 0)
            {
                break;
            }
            currentIndex++;
        }

        if (currentIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(currentIndex << 3);
            return cursor(currentIndex, indexValue(indexEntry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected long scanIndex(
        long cursor)
    {
        // assumes sorted by non-negative key
        final int cursorIndex = cursorIndex(cursor);
        assert cursorIndex >= 0;

        final DirectBuffer buffer = readableBuf;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (readCapacity >> 3) - 1;

        if (cursorIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(cursorIndex << 3);
            return cursor(cursorIndex, indexValue(indexEntry));
        }

        return lastIndex < maxIndex ? RETRY_SEGMENT : NEXT_SEGMENT;
    }

    protected long reverseScanValue(
        int key,
        long cursor)
    {
        // assumes sorted by (unsigned) value, repeated non-negative keys
        final int cursorIndex = cursorIndex(cursor);
        final int cursorValue = cursorValue(cursor);
        assert cursorIndex >= 0;

        final DirectBuffer buffer = readableBuf;
        final int capacity = readCapacity;
        final int maxIndex = (writeCapacity >> 3) - 1;
        final int lastIndex = (capacity >> 3) - 1;

        int currentIndex = Math.min(cursorIndex, lastIndex);
        while (currentIndex >= 0 && currentIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(currentIndex << 3);
            final int indexKey = indexKey(indexEntry);
            final int indexValue = indexValue(indexEntry);

            if (indexKey == key && compareUnsigned(indexValue, cursorValue) <= 0)
            {
                break;
            }
            currentIndex--;
        }

        if (currentIndex >= 0 && currentIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(currentIndex << 3);
            return cursor(currentIndex, indexValue(indexEntry));
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

                sort(buffer);

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

    protected void sortUnique(
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

                sort(buffer);
                final int newCapacity = unique(buffer);

                IoUtil.unmap(mapped);

                channel.truncate(newCapacity);
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

    private void sort(
        MutableDirectBuffer buffer)
    {
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
    }

    private int unique(
        MutableDirectBuffer buffer)
    {
        // assumes sorted
        final int capacity = buffer.capacity();

        int uniqueIndex = 0;
        int maxIndex = capacity - Long.BYTES;

        outer:
        for (int compareIndex = Long.BYTES; compareIndex <= maxIndex; )
        {
            while (buffer.getLong(compareIndex) == buffer.getLong(uniqueIndex))
            {
                compareIndex += Long.BYTES;

                if (compareIndex > maxIndex)
                {
                    break outer;
                }
            }

            assert buffer.getLong(compareIndex) != buffer.getLong(uniqueIndex);

            uniqueIndex += Long.BYTES;
            buffer.putLong(uniqueIndex, buffer.getLong(compareIndex));
            compareIndex += Long.BYTES;
        }

        uniqueIndex += Long.BYTES;

        return uniqueIndex;
    }
}
