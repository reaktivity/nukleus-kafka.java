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
package org.reaktivity.nukleus.kafka.internal.stream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;

public class HeadersMessageDispatcher implements MessageDispatcher
{
    static final HeadersMessageDispatcher NOOP = new HeadersMessageDispatcher(null)
    {
        @Override
        public void adjustOffset(int partition, long oldOffset, long newOffset)
        {
        }

        @Override
        public void detach(boolean reattach)
        {
        }

        @Override
        public void flush(int partition, long requestedOffset, long nextFetchOffset)
        {
        }

        @Override
        public int dispatch(
            int partition,
            long requestedOffset,
            long messageOffset,
            DirectBuffer key,
            Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
            long timestamp,
            long traceId,
            DirectBuffer value)
        {
            return 0;
        }
    };

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);
    private final Map<DirectBuffer, HeaderValueMessageDispatcher> dispatchersByHeaderKey = new HashMap<>();
    private final List<HeaderValueMessageDispatcher> dispatchers = new ArrayList<HeaderValueMessageDispatcher>();
    private final BroadcastMessageDispatcher broadcast = new BroadcastMessageDispatcher();
    private final Function<DirectBuffer, HeaderValueMessageDispatcher> createHeaderValueMessageDispatcher;

    private boolean deferUpdates;
    private boolean hasDeferredUpdates;

    HeadersMessageDispatcher(
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
        broadcast.adjustOffset(partition, oldOffset, newOffset);
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            dispatcher.adjustOffset(partition, oldOffset, newOffset);
        }
    }

    @Override
    public void detach(
        boolean reattach)
    {
        broadcast.detach(reattach);
        deferUpdates = true;
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            dispatcher.detach(reattach);
        }
        deferUpdates = false;

        processDeferredUpdates();
    }

    @Override
    public int dispatch(
         int partition,
         long requestedOffset,
         long messageOffset,
         DirectBuffer key,
         Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
         long timestamp,
         long traceId,
         DirectBuffer value)
    {
        int result = 0;
        result |=  broadcast.dispatch(partition, requestedOffset, messageOffset, key, supplyHeader, timestamp, traceId, value);
        deferUpdates = true;
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            result |= dispatcher.dispatch(partition, requestedOffset, messageOffset, key,
                        supplyHeader, timestamp, traceId, value);
        }
        deferUpdates = false;

        processDeferredUpdates();
        return result;
    }

    @Override
    public void flush(
        int partition,
        long requestedOffset,
        long nextFetchOffset)
    {
        broadcast.flush(partition, requestedOffset, nextFetchOffset);
        deferUpdates = true;
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            dispatcher.flush(partition, requestedOffset, nextFetchOffset);
        }
        deferUpdates = false;

        processDeferredUpdates();
    }

    public HeadersMessageDispatcher add(
                Iterator<KafkaHeaderFW> headers,
                MessageDispatcher dispatcher)
    {
        final KafkaHeaderFW header = headers.hasNext() ? headers.next() : null;
        if (header == null)
        {
            broadcast.add(dispatcher);
        }
        else
        {
            String16FW headerKey = header.key();
            int keyOffset = headerKey.offset() + Short.BYTES;
            int keyLength = headerKey.limit() - keyOffset;
            buffer.wrap(headerKey.buffer(), keyOffset, keyLength);
            HeaderValueMessageDispatcher valueDispatcher = dispatchersByHeaderKey.get(buffer);
            if (valueDispatcher == null)
            {
                int bytesLength = headerKey.sizeof() - Short.BYTES;
                UnsafeBuffer headerKeyCopy = new UnsafeBuffer(new byte[bytesLength]);
                headerKeyCopy.putBytes(0, headerKey.buffer(), keyOffset, keyLength);
                valueDispatcher = createHeaderValueMessageDispatcher.apply(headerKeyCopy);
                dispatchersByHeaderKey.put(headerKeyCopy, valueDispatcher);
                dispatchers.add(valueDispatcher);
            }
            valueDispatcher.add(header.value(), headers, dispatcher);
        }
        return this;
    }

    public boolean isEmpty()
    {
        return broadcast.isEmpty() &&
                 (dispatchers.isEmpty() || dispatchers.stream().allMatch(x -> x == HeaderValueMessageDispatcher.NOOP));
    }

    public boolean remove(
            Iterator<KafkaHeaderFW> headers,
            MessageDispatcher dispatcher)
    {
        boolean result = false;
        final KafkaHeaderFW header = headers.hasNext() ? headers.next() : null;
        if (header == null)
        {
            result = broadcast.remove(dispatcher);
        }
        else
        {
            String16FW headerKey = header.key();
            int valueOffset = headerKey.offset() + Short.BYTES;
            int valueLength = headerKey.limit() - valueOffset;
            buffer.wrap(headerKey.buffer(), valueOffset, valueLength);
            HeaderValueMessageDispatcher valueDispatcher = dispatchersByHeaderKey.get(buffer);
            if (valueDispatcher != null)
            {
                result = valueDispatcher.remove(header.value(), headers, dispatcher);
                if (valueDispatcher.isEmpty())
                {
                    if (deferUpdates)
                    {
                        dispatchersByHeaderKey.replace(buffer, HeaderValueMessageDispatcher.NOOP);
                        int index = dispatchers.indexOf(valueDispatcher);
                        if (index != -1)
                        {
                            dispatchers.set(index, HeaderValueMessageDispatcher.NOOP);
                        }
                        hasDeferredUpdates = true;
                    }
                    else
                    {
                        dispatchersByHeaderKey.remove(buffer);
                        dispatchers.remove(valueDispatcher);
                    }
                }
            }
        }
        return result;
    }

    private void processDeferredUpdates()
    {
        if (hasDeferredUpdates)
        {
            hasDeferredUpdates = false;
            dispatchersByHeaderKey.entrySet().removeIf(e -> e.getValue() == HeaderValueMessageDispatcher.NOOP);
            dispatchers.removeIf(e -> e == HeaderValueMessageDispatcher.NOOP);
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s, %s)", this.getClass().getSimpleName(), broadcast, toString(dispatchersByHeaderKey));
    }

    private <V> String toString(
        Map<DirectBuffer, V> map)
    {
        StringBuffer result = new StringBuffer(1000);
        result.append('{');
        for (Map.Entry<DirectBuffer, V> entry : map.entrySet())
        {
            result.append('\n');
            result.append(entry.getKey().getStringWithoutLengthUtf8(0, entry.getKey().capacity()));
            result.append(" = ");
            result.append(entry.getValue().toString());
        }
        result.append('}');
        return result.toString();
    }

}

