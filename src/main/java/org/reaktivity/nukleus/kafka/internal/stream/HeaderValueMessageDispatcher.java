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

public class HeaderValueMessageDispatcher implements MessageDispatcher
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);
    private final DirectBuffer headerName;

    private Map<UnsafeBuffer, HeadersMessageDispatcher> dispatchersByHeaderValue = new HashMap<>();

    public HeaderValueMessageDispatcher(DirectBuffer headerKey)
    {
        this.headerName = headerKey;
    }

    @Override
    public
    int dispatch(
             int partition,
             long requestOffset,
             long messageOffset,
             DirectBuffer key,
             Function<DirectBuffer, DirectBuffer> supplyHeader,
             DirectBuffer value)
    {
        int result = 0;
        DirectBuffer header = supplyHeader.apply(headerName);
        if (header != null)
        {
            buffer.wrap(header);
            MessageDispatcher dispatcher = dispatchersByHeaderValue.get(buffer);
            if (dispatcher != null)
            {
                result =  dispatcher.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, value);
            }
        }
        return result;
    }

    public void add(
            OctetsFW headerValue,
            ListFW<KafkaHeaderFW> headers,
            int index,
            MessageDispatcher dispatcher)
    {
        buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
        HeadersMessageDispatcher headersDispatcher = dispatchersByHeaderValue.get(buffer);
        if (headersDispatcher == null)
        {
            UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[headerValue.sizeof()]);
            keyCopy.putBytes(0,  headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
            headersDispatcher =  new HeadersMessageDispatcher();
            dispatchersByHeaderValue.put(keyCopy, headersDispatcher);
        }
        headersDispatcher.add(headers, index, dispatcher);
    }

    public boolean remove(
            OctetsFW headerValue,
            ListFW<KafkaHeaderFW> headers,
            int index,
            MessageDispatcher dispatcher)
    {
        boolean result = false;
        buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
        HeadersMessageDispatcher headersDispatcher = dispatchersByHeaderValue.get(buffer);
        if (headersDispatcher != null)
        {
            result = headersDispatcher.remove(headers, index, dispatcher);
            if (headersDispatcher.isEmpty())
            {
                dispatchersByHeaderValue.remove(buffer);
            }
        }
        return result;
    }

    public HeadersMessageDispatcher get(
            OctetsFW headerValue)
    {
        buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
        return dispatchersByHeaderValue.get(buffer);
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s, %s)", this.getClass().getSimpleName(), new String(headerName.byteArray()),
                dispatchersByHeaderValue);
    }

    public boolean isEmpty()
    {
         return dispatchersByHeaderValue.isEmpty();
    }

}
