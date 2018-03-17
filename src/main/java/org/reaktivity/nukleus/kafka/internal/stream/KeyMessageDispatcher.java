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
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;

public class KeyMessageDispatcher implements MessageDispatcher
{
    protected final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);

    private Map<UnsafeBuffer, HeadersMessageDispatcher> dispatchersByKey = new HashMap<>();

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
        buffer.wrap(key, 0, key.capacity());
        MessageDispatcher result = dispatchersByKey.get(buffer);
        return result == null ? 0 :
            result.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, value);
    }

    @Override
    public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
    {
        for (MessageDispatcher dispatcher: dispatchersByKey.values())
        {
            dispatcher.flush(partition, requestOffset, lastOffset);
        }
    }

    public void add(OctetsFW key, ListFW<KafkaHeaderFW> headers, MessageDispatcher dispatcher)
    {
        buffer.wrap(key.buffer(), key.offset(), key.sizeof());
        HeadersMessageDispatcher existing = dispatchersByKey.get(buffer);
        if (existing == null)
        {
            UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.sizeof()]);
            keyCopy.putBytes(0,  key.buffer(), key.offset(), key.sizeof());
            existing = new HeadersMessageDispatcher();
            dispatchersByKey.put(keyCopy, existing);
        }
        existing.add(headers, 0, dispatcher);
    }

    protected void flush(
            int partition,
            long requestOffset,
            long lastOffset,
            DirectBuffer key)
    {
        buffer.wrap(key, 0, key.capacity());
        MessageDispatcher dispatcher = dispatchersByKey.get(buffer);
        if (dispatcher != null)
        {
            dispatcher.flush(partition, requestOffset, lastOffset);
        }
    }

    public boolean remove(OctetsFW key, ListFW<KafkaHeaderFW> headers, MessageDispatcher dispatcher)
    {
        boolean result = false;
        buffer.wrap(key.buffer(), key.offset(), key.sizeof());
        HeadersMessageDispatcher headersDispatcher = dispatchersByKey.get(buffer);
        if (headersDispatcher != null)
        {
            result = headersDispatcher.remove(headers, 0, dispatcher);
            if (headersDispatcher.isEmpty())
            {
                dispatchersByKey.remove(buffer);
            }
        }
        return result;
    }

    public boolean isEmpty()
    {
        return dispatchersByKey.isEmpty();
    }

}
