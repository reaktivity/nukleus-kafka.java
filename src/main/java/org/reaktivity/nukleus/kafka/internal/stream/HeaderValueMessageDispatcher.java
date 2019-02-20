/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.DEBUG1;
import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.wrap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class HeaderValueMessageDispatcher implements MessageDispatcher
{
    static final HeaderValueMessageDispatcher NOOP = new HeaderValueMessageDispatcher("noop", null)
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
        public void flush(int partition, long requestOffset, long nextFetchOffset)
        {
        }

        @Override
        public int dispatch(
            int partition,
            long requestOffset,
            long messageStartOffset,
            DirectBuffer key,
            Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
            long timestamp,
            long traceId,
            DirectBuffer value)
        {
            return 0;
        }
    };

    final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);
    final DirectBuffer headerName;
    private final String topicName;
    Map<DirectBuffer, HeadersMessageDispatcher> dispatchersByHeaderValue = new HashMap<>();

    private final List<HeadersMessageDispatcher> dispatchers = new ArrayList<>();

    private boolean deferUpdates;
    private boolean hasDeferredUpdates;

    public HeaderValueMessageDispatcher(String topicName, DirectBuffer headerKey)
    {
        this.topicName = topicName;
        this.headerName = headerKey;
    }

    @Override
    public void adjustOffset(
        int partition,
        long oldOffset,
        long newOffset)
    {
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
        long requestOffset,
        long messageOffset,
        DirectBuffer key,
        Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
        long timestamp,
        long traceId,
        DirectBuffer value)
    {
        int result = 0;
        deferUpdates = true;
        Iterator<DirectBuffer> values = supplyHeader.apply(headerName);
        while (values.hasNext())
        {
            DirectBuffer header = values.next();
            buffer.wrap(header);
            MessageDispatcher dispatcher = dispatchersByHeaderValue.get(buffer);
            if (dispatcher != null)
            {
                result = dispatcher.dispatch(partition, requestOffset, messageOffset,
                                             key, supplyHeader, timestamp, traceId, value);
            }
        }
        deferUpdates = false;

        processDeferredUpdates();
        if (DEBUG1)
        {
            System.out.format("HVMD.dispatch: topic=%s partition=%d requestOffset=%d messageOffset=%d result=%d\n",
                    topicName,
                    partition, requestOffset, messageOffset, result);
        }
        return result;
    }

    @Override
    public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
    {
        deferUpdates = true;
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            dispatcher.flush(partition, requestOffset, lastOffset);
        }
        deferUpdates = false;

        processDeferredUpdates();
    }

    public void add(
            OctetsFW headerValue,
            Iterator<KafkaHeaderFW> headers,
            MessageDispatcher dispatcher)
    {
        wrap(buffer, headerValue);
        HeadersMessageDispatcher headersDispatcher = dispatchersByHeaderValue.get(buffer);
        if (headersDispatcher == null)
        {
            UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[headerValue.sizeof()]);
            keyCopy.putBytes(0,  headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
            headersDispatcher =  new HeadersMessageDispatcher(topicName, HeaderValueMessageDispatcher::new);
            dispatchersByHeaderValue.put(keyCopy, headersDispatcher);
            dispatchers.add(headersDispatcher);
        }
        headersDispatcher.add(headers, dispatcher);
    }

    public boolean remove(
            OctetsFW headerValue,
            Iterator<KafkaHeaderFW> headers,
            MessageDispatcher dispatcher)
    {
        boolean result = false;
        wrap(buffer, headerValue);
        HeadersMessageDispatcher headersDispatcher = dispatchersByHeaderValue.get(buffer);
        if (headersDispatcher != null)
        {
            result = headersDispatcher.remove(headers, dispatcher);
            if (headersDispatcher.isEmpty())
            {
                if (deferUpdates)
                {
                    dispatchersByHeaderValue.replace(buffer, HeadersMessageDispatcher.NOOP);
                    int index = dispatchers.indexOf(headersDispatcher);
                    if (index != -1)
                    {
                        dispatchers.set(index, HeadersMessageDispatcher.NOOP);
                    }
                    hasDeferredUpdates = true;
                }
                else
                {
                    dispatchersByHeaderValue.remove(buffer);
                    dispatchers.remove(headersDispatcher);
                }
                onRemoved(headersDispatcher);
            }
        }
        return result;
    }

    protected void onRemoved(
        MessageDispatcher headersDispatcher)
    {

    }

    public HeadersMessageDispatcher get(
            OctetsFW headerValue)
    {
        buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
        return dispatchersByHeaderValue.get(buffer);
    }

    private void processDeferredUpdates()
    {
        if (hasDeferredUpdates)
        {
            hasDeferredUpdates = false;
            dispatchersByHeaderValue.entrySet().removeIf(e -> e.getValue() == HeadersMessageDispatcher.NOOP);
            dispatchers.removeIf(e -> e == HeadersMessageDispatcher.NOOP);
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s, %s)", this.getClass().getSimpleName(), new String(headerName.byteArray()),
                toString(dispatchersByHeaderValue));
    }

    private <V> String toString(
        Map<DirectBuffer, V> map)
    {
        StringBuffer result = new StringBuffer(1000);
        result.append("{");
        boolean first = true;
        for (Map.Entry<DirectBuffer, V> entry : map.entrySet())
        {
            if (first)
            {
                first = false;
            }
            else
            {
                result.append(", ");
            }
            result.append(entry.getKey().getStringWithoutLengthUtf8(0, entry.getKey().capacity()));
            result.append("=");
            result.append(entry.getValue().toString());
        }
        result.append("}");
        return result.toString();
    }

    public boolean isEmpty()
    {
         return dispatchers.isEmpty() || dispatchers.stream().allMatch(x -> x == HeadersMessageDispatcher.NOOP);
    }

}
