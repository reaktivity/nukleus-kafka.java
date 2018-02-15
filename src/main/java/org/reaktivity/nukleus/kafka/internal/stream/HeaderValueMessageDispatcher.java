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
import java.util.function.Supplier;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;

public class HeaderValueMessageDispatcher implements MessageDispatcher
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[0]);
    private final UnsafeBuffer headerName;

    private Map<UnsafeBuffer, HeaderNameMessageDispatcher> dispatchersByKey = new HashMap<>();

    public HeaderValueMessageDispatcher(String16FW headerKey)
    {
        int bytesLength = headerKey.sizeof() - Short.BYTES;
        this.headerName = new UnsafeBuffer(new byte[bytesLength]);
        this.headerName.putBytes(0, headerKey.buffer(), headerKey.offset() + Short.BYTES, bytesLength);
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
        DirectBuffer header = supplyHeader.apply(headerName);
        buffer.wrap(header);
        MessageDispatcher result = dispatchersByKey.get(buffer);
        return result == null ? 0 :
            result.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, value);
    }

    public HeaderNameMessageDispatcher computeIfAbsent(
            OctetsFW headerValue,
            Supplier<HeaderNameMessageDispatcher> dispatcher)
    {
        buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
        HeaderNameMessageDispatcher existing = dispatchersByKey.get(buffer);
        if (existing == null)
        {
            UnsafeBuffer keyCopy = new UnsafeBuffer(new byte[headerValue.sizeof()]);
            keyCopy.putBytes(0,  headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
            existing = dispatcher.get();
            dispatchersByKey.put(keyCopy, existing);
        }
        return existing;
    }

    public HeaderNameMessageDispatcher remove(OctetsFW headerValue)
    {
        buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
        return dispatchersByKey.remove(buffer);
    }

    public HeaderNameMessageDispatcher get(
            OctetsFW headerValue)
    {
        buffer.wrap(headerValue.buffer(), headerValue.offset(), headerValue.sizeof());
        return dispatchersByKey.get(buffer);
    }

}
