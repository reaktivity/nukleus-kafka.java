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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class PartitionCache
{
    private static final long NONE = -1L;
    private final Map<UnsafeBuffer, long[]> offsetsByKey = new HashMap<>();
    private final long[] offsets;
    //private Headers[] headers;

    PartitionCache(int initialCapacity)
    {
        offsets = new long[initialCapacity];
    }

    void add(
        long messageOffset,
        DirectBuffer key,
        //Headers headers,
        long timestamp,
        long traceId,
        DirectBuffer value)
    {

    }

    private void remove(
        long offset)
    {
        int index = locate(offset);
        if (offsets[index] == offset)
        {
            offsets[index] = NONE;
        }
    }

    private int locate(
        long offset)
    {
        return Arrays.binarySearch(offsets, offset);
    }

}
