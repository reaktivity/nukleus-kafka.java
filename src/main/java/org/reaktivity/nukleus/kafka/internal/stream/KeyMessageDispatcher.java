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
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

import static org.reaktivity.nukleus.kafka.internal.stream.HeadersMessageDispatcher.NOOP;

public class KeyMessageDispatcher implements MessageDispatcher
{
    protected final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);
    private final Function<DirectBuffer, HeaderValueMessageDispatcher> createHeaderValueMessageDispatcher;

    private Map<UnsafeBuffer, HeadersMessageDispatcher> dispatchersByKey = new HashMap<>();

    boolean deferUpdates;
    private boolean hasDeferredUpdates;

    public KeyMessageDispatcher(
        Function<DirectBuffer, HeaderValueMessageDispatcher> createHeaderValueMessageDispatcher)
    {
        this.createHeaderValueMessageDispatcher = createHeaderValueMessageDispatcher;
    }

    @Override
    public void adjustOffset(
        int partition,
        long oldOffset,
        long newOffset)
    {
        for (MessageDispatcher dispatcher: dispatchersByKey.values())
        {
            dispatcher.adjustOffset(partition, oldOffset, newOffset);
        }
    }

    @Override
    public void detach(
        boolean reattach)
    {
        deferUpdates = true;
        for (MessageDispatcher dispatcher: dispatchersByKey.values())
        {
            dispatcher.detach(reattach);
        }
        deferUpdates = false;

        processDeferredUpdates();
    }

    @Override
    public int dispatch(
        int partition,
        long requestOffset,
        long messageOffset,
        DirectBuffer key,
        Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
        long timestamp,
        long traceId,
        DirectBuffer value)
    {
        buffer.wrap(key, 0, key.capacity());
        MessageDispatcher result = dispatchersByKey.get(buffer);
        return result == null ? 0 :
            result.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, traceId, value);
    }

    @Override
    public void flush(
        int partition,
        long requestOffset,
        long lastOffset)
    {
        deferUpdates = true;
        for (MessageDispatcher dispatcher: dispatchersByKey.values())
        {
            dispatcher.flush(partition, requestOffset, lastOffset);
        }
        deferUpdates = false;

        processDeferredUpdates();
    }

    public long latestOffset(
        int partition,
        OctetsFW key)
    {
        return 0L;
    }

    public long lowestOffset(
        int partition)
    {
        return 0L;
    }

    public boolean shouldDispatch(
        DirectBuffer key,
        long messageOffset)
    {
        return true;
    }

    public void add(OctetsFW key, Iterator<KafkaHeaderFW> headers, MessageDispatcher dispatcher)
    {
        buffer.wrap(key.buffer(), key.offset(), key.sizeof());
        HeadersMessageDispatcher existing = dispatchersByKey.get(buffer);
        if (existing == null)
        {
            UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[key.sizeof()]);
            keyCopy.putBytes(0,  key.buffer(), key.offset(), key.sizeof());
            existing = new HeadersMessageDispatcher(createHeaderValueMessageDispatcher);
            dispatchersByKey.put(keyCopy, existing);
        }
        existing.add(headers, dispatcher);
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

    public boolean remove(OctetsFW key, Iterator<KafkaHeaderFW> headers, MessageDispatcher dispatcher)
    {
        boolean result = false;
        buffer.wrap(key.buffer(), key.offset(), key.sizeof());
        HeadersMessageDispatcher headersDispatcher = dispatchersByKey.get(buffer);
        if (headersDispatcher != null)
        {
            result = headersDispatcher.remove(headers, dispatcher);
            if (headersDispatcher.isEmpty())
            {
                if (deferUpdates)
                {
                    dispatchersByKey.replace(buffer, NOOP);
                    hasDeferredUpdates = true;
                }
                else
                {
                    dispatchersByKey.remove(buffer);
                }
            }
        }
        return result;
    }

    public boolean isEmpty()
    {
        return dispatchersByKey.isEmpty() || dispatchersByKey.values().stream().allMatch(x -> x == HeadersMessageDispatcher.NOOP);
    }

    private void processDeferredUpdates()
    {
        if (hasDeferredUpdates)
        {
            hasDeferredUpdates = false;
            dispatchersByKey.entrySet().removeIf(e -> e.getValue() == NOOP);
        }
    }

}
