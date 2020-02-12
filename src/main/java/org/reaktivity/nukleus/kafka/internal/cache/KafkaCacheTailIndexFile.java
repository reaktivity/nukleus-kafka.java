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
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursor;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorIndex;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheIndexRecord.indexKey;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheIndexRecord.indexValue;

import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheTailSegment;

public abstract class KafkaCacheTailIndexFile extends KafkaCacheTailFile
{
    protected KafkaCacheTailIndexFile(
        KafkaCacheTailSegment segment,
        String extension)
    {
        super(segment, extension);
    }

    protected Int2IntHashMap toMap()
    {
        final Int2IntHashMap map = new Int2IntHashMap(-1);
        for (int index = 0; index < readCapacity; index += Long.BYTES)
        {
            final long entry = readableBuf.getLong(index);
            map.put(indexKey(entry), indexValue(entry));
        }
        return map;
    }

    protected long seekKey(
        int key)
    {
        // assumes sorted by key
        final DirectBuffer buffer = readableBuf;
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
                while (lowIndex < lastIndex)
                {
                    final int candidateIndex = lowIndex + 1;
                    assert candidateIndex <= lastIndex;

                    final long candidateEntry = buffer.getLong(candidateIndex << 3);
                    final int candidateKey = indexKey(candidateEntry);

                    if (candidateKey != key)
                    {
                        break;
                    }

                    lowIndex = candidateIndex;
                }

                assert lowIndex <= lastIndex;
                break;
            }
        }

        assert lowIndex >= 0;
        if (lowIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(lowIndex << 3);
            return cursor(lowIndex, indexValue(indexEntry));
        }

        return NEXT_SEGMENT;
    }

    protected long scanKey(
        int key,
        long cursor)
    {
        // assumes sorted by key, repeated keys, record from seekKey
        // TODO: optimize scanKey to break loop on key mismatch
        //       requires cursor condition retain memento of last match index
        final int index = cursorIndex(cursor);
        final int value = cursorValue(cursor);
        assert index >= 0;

        final DirectBuffer buffer = readableBuf;
        final int capacity = readCapacity;
        final int lastIndex = (capacity >> 3) - 1;

        int currentIndex = index;
        while (currentIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(currentIndex << 3);
            final int indexKey = indexKey(indexEntry);
            final int indexValue = indexValue(indexEntry);
            if (indexKey == key && compareUnsigned(indexValue, value) >= 0)
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

        return NEXT_SEGMENT;
    }

    protected long reverseSeekKey(
        int key)
    {
        // assumes sorted by key
        final DirectBuffer buffer = readableBuf;
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
                while (lowIndex > 0)
                {
                    final int candidateIndex = lowIndex - 1;
                    assert candidateIndex >= 0;

                    final long candidateEntry = buffer.getLong(candidateIndex << 3);
                    final int candidateKey = indexKey(candidateEntry);

                    if (candidateKey != key)
                    {
                        break;
                    }

                    lowIndex = candidateIndex;
                }

                assert lowIndex <= lastIndex;
                break;
            }
        }

        assert lowIndex >= 0;
        if (lowIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(lowIndex << 3);
            return cursor(lowIndex, indexValue(indexEntry));
        }

        return NEXT_SEGMENT;
    }

    protected long reverseScanKey(
        int key,
        long cursor)
    {
        // assumes sorted by key, repeated keys, record from seekKey
        // TODO: optimize reverseScanKey to break loop on key mismatch
        //       requires cursor condition retain memento of last match index
        final int cursorIndex = cursorIndex(cursor);
        final int cursorValue = cursorValue(cursor);
        assert cursorIndex >= 0;

        final DirectBuffer buffer = readableBuf;
        final int capacity = readCapacity;
        final int lastIndex = (capacity >> 3) - 1;

        int currentIndex = cursorIndex;
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

        return NEXT_SEGMENT;
    }

    protected long scanIndex(
        long cursor)
    {
        final int cursorIndex = cursorIndex(cursor);
        assert cursorIndex >= 0;

        final DirectBuffer buffer = readableBuf;
        final int lastIndex = (readCapacity >> 3) - 1;

        if (cursorIndex <= lastIndex)
        {
            final long indexEntry = buffer.getLong(cursorIndex << 3);
            return cursor(cursorIndex, indexValue(indexEntry));
        }

        return NEXT_SEGMENT;
    }
}
