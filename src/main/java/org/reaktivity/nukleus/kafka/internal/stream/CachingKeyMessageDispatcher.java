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
package org.reaktivity.nukleus.kafka.internal.stream;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class CachingKeyMessageDispatcher extends KeyMessageDispatcher
{
    private final Map<UnsafeBuffer, long[]> offsetsByKey = new HashMap<>();

    private long highestOffset;

    @Override
    public int dispatch(
         int partition,
         long requestOffset,
         long messageOffset,
         DirectBuffer key,
         Function<DirectBuffer, DirectBuffer> supplyHeader,
         long timestamp,
         DirectBuffer value)
    {
        long messageStartOffset = messageOffset - 1;
        buffer.wrap(key, 0, key.capacity());
        long[] offset = offsetsByKey.get(buffer);
        if (offset == null)
        {
            UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.capacity()]);
            keyCopy.putBytes(0,  key, 0, key.capacity());
            offsetsByKey.put(keyCopy, new long[]{messageStartOffset});
        }
        else
        {
            offset[0] = Math.max(messageStartOffset, offset[0]);
        }

        // highestOffset must only be incremented if we originally queried from offset zero
        // so it can be used as the starting offset for absent keys
        if (requestOffset <= highestOffset && messageOffset > highestOffset)
        {
            highestOffset = messageOffset;
        }

        int dispatched = super.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, value);

        // detect historical message stream
        if (dispatched > 0 && messageOffset < highestOffset)
        {
            buffer.wrap(key, 0, key.capacity());
            offset = offsetsByKey.get(buffer);

            // fast-forward to live stream after observing most recent cached offset for message key
            if (offset != null  && offset[0] == messageStartOffset)
            {
                super.flush(partition, requestOffset, highestOffset);
            }
        }
        return dispatched;
    }

    @Override
    public long lastOffset(
        int partition,
        OctetsFW key)
    {
        buffer.wrap(key.buffer(), key.offset(), key.sizeof());
        long[] offset = offsetsByKey.get(buffer);
        return offset == null ? highestOffset : offset[0];
    }

}
