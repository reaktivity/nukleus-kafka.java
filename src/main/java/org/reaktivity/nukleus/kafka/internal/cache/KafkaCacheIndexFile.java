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
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
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
import java.util.Arrays;
import java.util.function.IntFunction;

import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public abstract class KafkaCacheIndexFile extends KafkaCacheFile
{

    protected KafkaCacheIndexFile(
        Path location,
        int capacity,
        MutableDirectBuffer appendBuf)
    {
        super(location, capacity, appendBuf);
    }

    protected KafkaCacheIndexFile(
        Path location)
    {
        super(location);
    }

    public abstract long first(int key);
    public abstract long last(int key);

    public long resolve(
        long cursor)
    {
        final int index = cursorIndex(cursor);
        assert index >= 0;

        long resolve = available() != 0 ? RETRY_SEGMENT : NEXT_SEGMENT;

        final int lastIndex = (capacity() >> 3) - 1;

        if (index <= lastIndex)
        {
            final long indexEntry = readLong(index << 3);
            resolve = cursor(index, indexValue(indexEntry));
        }

        return resolve;
    }

    public abstract long higher(int key, long cursor);
    public abstract long ceiling(int key, long cursor);

    public abstract long floor(int key, long cursor);
    public abstract long lower(int key, long cursor);

    public static class SortedByKey extends KafkaCacheIndexFile
    {
        protected SortedByKey(
            Path location,
            int capacity,
            MutableDirectBuffer appendBuf)
        {
            super(location, capacity, appendBuf);
        }

        protected SortedByKey(
            Path location)
        {
            super(location);
        }

        @Override
        public long first(
            int key)
        {
            long first = available() != 0 ? RETRY_SEGMENT : NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;

            int lowIndex = 0;
            int highIndex = lastIndex;

            while (lowIndex <= highIndex)
            {
                final int midIndex = (lowIndex + highIndex) >>> 1;
                final long midEntry = readLong(midIndex << 3);
                final int midKey = indexKey(midEntry);
                final int compareKey = compareUnsigned(midKey, key);

                if (compareKey < 0)
                {
                    lowIndex = midIndex + 1;
                }
                else if (compareKey > 0)
                {
                    highIndex = midIndex - 1;
                }
                else
                {
                    long lowEntry;

                    lowIndex = midIndex;
                    lowEntry = midEntry;

                    while (lowIndex > 0)
                    {
                        final int candidateIndex = lowIndex - 1;
                        assert candidateIndex <= lastIndex;

                        final long candidateEntry = readLong(candidateIndex << 3);
                        final int candidateKey = indexKey(candidateEntry);

                        if (candidateKey != key)
                        {
                            break;
                        }

                        lowIndex = candidateIndex;
                        lowEntry = candidateEntry;
                    }

                    assert 0 <= lowIndex && lowIndex <= midIndex;

                    first = cursor(lowIndex, indexValue(lowEntry));
                    break;
                }
            }

            return first;
        }

        @Override
        public long last(
            int key)
        {
            long last = NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;

            int lowIndex = 0;
            int highIndex = lastIndex;

            while (lowIndex <= highIndex)
            {
                final int midIndex = (lowIndex + highIndex) >>> 1;
                final long midEntry = readLong(midIndex << 3);
                final int midKey = indexKey(midEntry);
                final int compareKey = compareUnsigned(midKey, key);

                if (compareKey < 0)
                {
                    lowIndex = midIndex + 1;
                }
                else if (compareKey > 0)
                {
                    highIndex = midIndex - 1;
                }
                else
                {
                    long highEntry;

                    highIndex = midIndex;
                    highEntry = midEntry;

                    while (highIndex < lastIndex)
                    {
                        final int candidateIndex = highIndex + 1;
                        assert candidateIndex >= 0;

                        final long candidateEntry = readLong(candidateIndex << 3);
                        final int candidateKey = indexKey(candidateEntry);

                        if (candidateKey != key)
                        {
                            break;
                        }

                        highIndex = candidateIndex;
                        highEntry = candidateEntry;
                    }

                    assert midIndex <= highIndex && highIndex <= lastIndex;

                    last = cursor(highIndex, indexValue(highEntry));
                    break;
                }
            }

            return last;
        }

        @Override
        public long higher(
            int key,
            long cursor)
        {
            // TODO: optimize to break loop on key mismatch
            //       requires cursor condition retain memento of last match index
            final int index = cursorIndex(cursor);
            final int value = cursorValue(cursor);
            assert index >= 0;

            long higher = available() != 0 ? RETRY_SEGMENT : NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = index; currentIndex <= lastIndex; currentIndex++)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);
                final int indexValue = indexValue(indexEntry);

                if (indexKey == key && compareUnsigned(indexValue, value) > 0)
                {
                    higher = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return higher;
        }

        @Override
        public long ceiling(
            int key,
            long cursor)
        {
            // TODO: optimize to break loop on key mismatch
            //       requires cursor condition retain memento of last match index
            final int index = cursorIndex(cursor);
            final int value = cursorValue(cursor);
            assert index >= 0;

            long ceiling = available() != 0 ? RETRY_SEGMENT : NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = index; currentIndex <= lastIndex; currentIndex++)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);
                final int indexValue = indexValue(indexEntry);

                if (indexKey == key && compareUnsigned(indexValue, value) >= 0)
                {
                    ceiling = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return ceiling;
        }

        @Override
        public long floor(
            int key,
            long cursor)
        {
            // TODO: optimize to break loop on key mismatch
            //       requires cursor condition retain memento of last match index
            final int index = cursorIndex(cursor);
            final int value = cursorValue(cursor);

            long floor = NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = index; 0 <= currentIndex && currentIndex <= lastIndex; currentIndex--)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);
                final int indexValue = indexValue(indexEntry);

                if (indexKey == key && compareUnsigned(indexValue, value) <= 0)
                {
                    floor = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return floor;
        }

        @Override
        public long lower(
            int key,
            long cursor)
        {
            // TODO: optimize to break loop on key mismatch
            //       requires cursor condition retain memento of last match index
            final int index = cursorIndex(cursor);
            final int value = cursorValue(cursor);

            long lower = NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = index; 0 <= currentIndex && currentIndex <= lastIndex; currentIndex--)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);
                final int indexValue = indexValue(indexEntry);

                if (indexKey == key && compareUnsigned(indexValue, value) < 0)
                {
                    lower = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return lower;
        }
    }

    public static class SortedByValue extends KafkaCacheIndexFile
    {
        private final IntFunction<long[]> sortSpaceRef;

        protected SortedByValue(
            Path location,
            int capacity,
            MutableDirectBuffer appendBuf,
            IntFunction<long[]> sortSpaceRef)
        {
            super(location, capacity, appendBuf);
            this.sortSpaceRef = sortSpaceRef;
        }

        protected SortedByValue(
            Path location,
            IntFunction<long[]> sortSpaceRef)
        {
            super(location);
            this.sortSpaceRef = sortSpaceRef;
        }

        @Override
        public long first(
            int key)
        {
            long first = available() != 0 ? RETRY_SEGMENT : NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = 0; currentIndex <= lastIndex; currentIndex++)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);

                if (indexKey == key)
                {
                    first = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return first;
        }

        @Override
        public long last(
            int key)
        {
            long last = NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = lastIndex; currentIndex >= 0; currentIndex--)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);

                if (indexKey == key)
                {
                    last = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return last;
        }

        @Override
        public long higher(
            int key,
            long cursor)
        {
            final int index = cursorIndex(cursor);
            final int value = cursorValue(cursor);
            assert index >= 0;

            long higher = available() != 0 ? RETRY_SEGMENT : NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = index; currentIndex <= lastIndex; currentIndex++)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);
                final int indexValue = indexValue(indexEntry);

                if (indexKey == key && compareUnsigned(indexValue, value) > 0)
                {
                    higher = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return higher;
        }

        @Override
        public long ceiling(
            int key,
            long cursor)
        {
            final int index = cursorIndex(cursor);
            final int value = cursorValue(cursor);
            assert index >= 0;

            long ceiling = available() != 0 ? RETRY_SEGMENT : NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = index; currentIndex <= lastIndex; currentIndex++)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);
                final int indexValue = indexValue(indexEntry);

                if (indexKey == key && compareUnsigned(indexValue, value) >= 0)
                {
                    ceiling = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return ceiling;
        }

        @Override
        public long floor(
            int key,
            long cursor)
        {
            final int index = cursorIndex(cursor);
            final int value = cursorValue(cursor);
            assert index >= 0;

            long floor = NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = index; 0 <= currentIndex && currentIndex <= lastIndex; currentIndex--)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);
                final int indexValue = indexValue(indexEntry);

                if (indexKey == key && compareUnsigned(indexValue, value) <= 0)
                {
                    floor = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return floor;
        }

        @Override
        public long lower(
            int key,
            long cursor)
        {
            final int index = cursorIndex(cursor);
            final int value = cursorValue(cursor);

            long lower = NEXT_SEGMENT;

            final int lastIndex = (capacity() >> 3) - 1;
            for (int currentIndex = index; 0 <= currentIndex && currentIndex <= lastIndex; currentIndex--)
            {
                final long indexEntry = readLong(currentIndex << 3);
                final int indexKey = indexKey(indexEntry);
                final int indexValue = indexValue(indexEntry);

                if (indexKey == key && compareUnsigned(indexValue, value) < 0)
                {
                    lower = cursor(currentIndex, indexValue(indexEntry));
                    break;
                }
            }

            return lower;
        }

        protected void sortByKey(
            Path workingFile,
            Path sortedFile)
        {
            try
            {
                final Path unsortedFile = location();
                Files.copy(unsortedFile, workingFile, REPLACE_EXISTING);

                try (FileChannel channel = FileChannel.open(workingFile, READ, WRITE))
                {
                    final ByteBuffer mapped = channel.map(MapMode.READ_WRITE, 0, channel.size());
                    final MutableDirectBuffer buffer = new UnsafeBuffer(mapped);

                    sortByKey(buffer);

                    IoUtil.unmap(mapped);
                }
                catch (IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }

                Files.move(workingFile, sortedFile, REPLACE_EXISTING);
                Files.delete(unsortedFile);
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        protected void sortByKeyUnique(
            Path workingFile,
            Path sortedFile)
        {
            try
            {
                final Path unsortedFile = location();
                Files.copy(unsortedFile, workingFile, REPLACE_EXISTING);

                try (FileChannel channel = FileChannel.open(workingFile, READ, WRITE))
                {
                    final ByteBuffer mapped = channel.map(MapMode.READ_WRITE, 0, channel.size());
                    final MutableDirectBuffer buffer = new UnsafeBuffer(mapped);

                    sortByKey(buffer);
                    final int newCapacity = unique(buffer);

                    IoUtil.unmap(mapped);

                    channel.truncate(newCapacity);
                }
                catch (IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }

                Files.move(workingFile, sortedFile, REPLACE_EXISTING);
                Files.delete(unsortedFile);
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        private void sortByKey(
            MutableDirectBuffer buffer)
        {
            final int capacity = buffer.capacity();
            final int length = capacity >> 3;

            final long[] sortSpace = sortSpaceRef.apply(length);
            assert sortSpace != null && length <= sortSpace.length;

            for (int index = 0, offset = 0; index < length; index++, offset += Long.BYTES)
            {
                sortSpace[index] = buffer.getLong(offset);
            }

            Arrays.sort(sortSpace, 0, length);

            for (int index = 0, offset = 0; index < length; index++, offset += Long.BYTES)
            {
                buffer.putLong(offset, sortSpace[index]);
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
}
