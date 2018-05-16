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
import java.util.Map.Entry;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class CompactedHeaderValueMessageDispatcher extends HeaderValueMessageDispatcher
{
    private final Map<UnsafeBuffer, HeadersMessageDispatcher> dispatchersByKey = new HashMap<>();

    public CompactedHeaderValueMessageDispatcher(DirectBuffer headerKey)
    {
        super(headerKey);
    }

    @Override
    public byte dispatch(
         int partition,
         long requestOffset,
         long messageOffset,
         DirectBuffer key,
         Function<DirectBuffer, DirectBuffer> supplyHeader,
         long timestamp,
         long traceId,
         DirectBuffer value)
    {
        byte result = 0;
        DirectBuffer headerValue = supplyHeader.apply(headerName);
        MessageDispatcher previousDispatcher = dispatchersByKey.get(key);
        HeadersMessageDispatcher dispatcher = null;
        if (headerValue != null)
        {
            buffer.wrap(headerValue);
            dispatcher = dispatchersByHeaderValue.get(buffer);
            if (dispatcher != null)
            {
                saveDispatcher(key, dispatcher);
                result = MessageDispatcher.FLAGS_MATCHED;
                result |= dispatcher.dispatch(partition, requestOffset, messageOffset,
                                              key, supplyHeader, timestamp, traceId, value);
            }
        }
        if (previousDispatcher != null && previousDispatcher != dispatcher)
        {
            // Send tombstone message
            result |= previousDispatcher.dispatch(partition, requestOffset, messageOffset,
                    key, supplyHeader, timestamp, traceId, null);
        }
        if (previousDispatcher != null && dispatcher == null)
        {
            buffer.wrap(key);
            dispatchersByKey.remove(buffer);
        }
        return result;
    }

    @Override
    protected void onRemoved(
        HeadersMessageDispatcher headersDispatcher)
    {
        Iterator<Entry<UnsafeBuffer, HeadersMessageDispatcher>> entries = dispatchersByKey.entrySet().iterator();
        while (entries.hasNext())
        {
            Entry<UnsafeBuffer, HeadersMessageDispatcher> entry = entries.next();
            if (headersDispatcher == entry.getValue())
            {
                entries.remove();
            }
        }
    }

    int dispatchersByKeySize()
    {
        return dispatchersByKey.size();
    }

    private UnsafeBuffer makeCopy(
        DirectBuffer source)
    {
        UnsafeBuffer copy = new UnsafeBuffer(new byte[source.capacity()]);
        copy.putBytes(0,  source, 0, source.capacity());
        return copy;
    }

    private void saveDispatcher(
        DirectBuffer key,
        HeadersMessageDispatcher dispatcher)
    {
        if (key != null)
        {
            buffer.wrap(key);
            if (dispatchersByKey.containsKey(buffer))
            {
                // allocation of key copy not needed
                dispatchersByKey.put(buffer, dispatcher);
            }
            else
            {
                dispatchersByKey.put(makeCopy(key), dispatcher);
            }
        }
    }

}
