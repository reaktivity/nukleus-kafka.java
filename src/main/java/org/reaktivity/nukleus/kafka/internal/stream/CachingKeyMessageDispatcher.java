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

        if (requestOffset <= highestOffset && messageOffset > highestOffset)
        {
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
                offset[0] = messageStartOffset;
            }
            highestOffset = messageOffset;
        }

        int dispatched = super.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, value);
        if (dispatched > 0 && messageOffset < highestOffset)
        {
            buffer.wrap(key, 0, key.capacity());
            long[] offset = offsetsByKey.get(buffer);
            if (offset != null  && offset[0] == messageStartOffset)
            {
                // The message is a historical one which is the latest one with this key.
                // Tell consumers to move up to the live stream.
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
        return offset == null ? 0 : offset[0];
    }

}
