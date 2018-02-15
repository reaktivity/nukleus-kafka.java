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

public class HeaderNameMessageDispatcher implements MessageDispatcher
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);
    private final Map<UnsafeBuffer, HeaderValueMessageDispatcher> dispatchersByHeaderName = new HashMap<>();
    private final BroadcastMessageDispatcher broadcast = new BroadcastMessageDispatcher();

    public HeaderNameMessageDispatcher()
    {

    }

    @Override
    public int dispatch(
                 int partition,
                 long requestOffset,
                 long messageOffset,
                 DirectBuffer key,
                 Function<DirectBuffer, DirectBuffer> supplyHeader,
                 DirectBuffer value)
    {
        int result = 0;
        for (MessageDispatcher dispatcher : dispatchersByHeaderName.values())
        {
            dispatcher.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, value);
        }
        result +=  broadcast.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, value);
        return result;
    }

    public HeaderNameMessageDispatcher add(
                ListFW<KafkaHeaderFW> headers,
                int index,
                MessageDispatcher dispatcher)
    {
        final int[] counter = new int[]{0};
        final KafkaHeaderFW header = headers.matchFirst(h -> (index == counter[0]++));
        if (header == null)
        {
            broadcast.add(dispatcher);
        }
        else
        {
            OctetsFW headerValue = header.value();
            buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
            HeaderValueMessageDispatcher valueDispatcher = dispatchersByHeaderName.get(buffer);
            if (valueDispatcher == null)
            {
                UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[headerValue.sizeof()]);
                keyCopy.putBytes(0,  headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
                valueDispatcher = new HeaderValueMessageDispatcher(header.key());
                dispatchersByHeaderName.put(keyCopy, valueDispatcher);
            }
            valueDispatcher.computeIfAbsent(headerValue, () -> new HeaderNameMessageDispatcher())
                 .add(headers, index + 1, dispatcher);
        }
        return this;
    }

    public boolean remove(
            ListFW<KafkaHeaderFW> headers,
            int index,
            MessageDispatcher dispatcher)
    {
        final int[] counter = new int[]{0};
        final KafkaHeaderFW header = headers.matchFirst(h -> (index == counter[0]++));
        if (header == null)
        {
            broadcast.remove(dispatcher);
        }
        else
        {
            OctetsFW headerValue = header.value();
            buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
            HeaderValueMessageDispatcher valueDispatcher = dispatchersByHeaderName.get(buffer);
            if (valueDispatcher != null)
            {
                HeaderNameMessageDispatcher nameDispatcher = valueDispatcher.get(headerValue);
                if (nameDispatcher.remove(headers, index + 1, dispatcher))
                {
                    dispatchersByHeaderName.remove(buffer);
                }
            }
        }
        return broadcast.isEmpty() && dispatchersByHeaderName.isEmpty();
    }

}

