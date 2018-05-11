/**
 * Copyright 2016-2017 The Reaktivity Project
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

import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.EMPTY_BYTE_ARRAY;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;

public class PartitionCache
{
    private static final long NO_OFFSET = -1L;
    private static final int NO_MESSAGE = -1;

    private final MessageCache messageCache;
    private final Map<UnsafeBuffer, long[]> offsetsByKey;
    private final long[] offsets;
    private final int[] messages;

    private final UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private int entries;
    private int size;

    PartitionCache(
        int initialCapacity,
        MessageCache messageCache)
    {
        this.messageCache = messageCache;
        this.offsetsByKey = new HashMap<>(initialCapacity);
        offsets = new long[initialCapacity];
        messages = new int[initialCapacity];
    }

    void add(
        long requestOffset,
        long nextFetchOffset,
        DirectBuffer key,
        HeadersFW headers,
        long timestamp,
        long traceId,
        DirectBuffer value)
    {
        long messageOffset = nextFetchOffset - 1;
        long highestOffset = offsets[size - 1];
        int message = NO_MESSAGE;

        if (messageOffset > highestOffset)
        {
            ensureCapacity();
            buffer.wrap(key, 0, key.capacity());
            if (value == null)
            {
                offsetsByKey.remove(buffer);
            }
            else
            {
                long[] offset = offsetsByKey.get(buffer);
                if (offset == null)
                {
                    UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.capacity()]);
                    keyCopy.putBytes(0,  key, 0, key.capacity());
                    offsetsByKey.put(keyCopy, new long[]{messageOffset});
                }
                else
                {
                    offset[0] = Math.max(messageOffset, offset[0]);
                }
            }
            // TODO: cache the message offset and message
            // int index = messageCache.put(key, headers, timestamp, traceId, value);
        }
        else
        {
            // cache the message if not already cached
        }
    }



    private void ensureCapacity()
    {
        // TODO: if insufficient capacity:
        //           compact if worthwhile
        //           reallocate if still necessary
    }

    private void remove(
        long offset)
    {
        int index = locate(offset);
        if (offsets[index] == offset)
        {
            offsets[index] = NO_OFFSET;
            messages[index] = NO_MESSAGE;
        }
    }

    private int locate(
        long offset)
    {
        return Arrays.binarySearch(offsets, offset);
    }

}
