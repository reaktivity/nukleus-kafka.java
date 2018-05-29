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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.UnsafeBuffer;

public class CompactedHeaderValueMessageDispatcher extends HeaderValueMessageDispatcher
{
    private final Map<UnsafeBuffer, MessageDispatcher[]> dispatchersByKey = new HashMap<>();

    private final List<MessageDispatcher> dispatchers = new ArrayList<>();

    public CompactedHeaderValueMessageDispatcher(DirectBuffer headerKey)
    {
        super(headerKey);
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
        // TODO: there can be multiple dispatchers for a key when there are multiple header values!
        HeadersMessageDispatcher dispatcher = null;
        Iterator<DirectBuffer> headerValues = supplyHeader.apply(headerName);
        dispatchers.clear();
        while (headerValues.hasNext())
        {
            DirectBuffer headerValue = headerValues.next();
            buffer.wrap(headerValue);
            dispatcher = dispatchersByHeaderValue.get(buffer);
            if (dispatcher != null)
            {
                dispatchers.add(dispatcher);
                result = MessageDispatcher.FLAGS_MATCHED;
                result |= dispatcher.dispatch(partition, requestOffset, messageOffset,
                                              key, supplyHeader, timestamp, traceId, value);
            }
        }
        if (key != null)
        {
            result = updateDispatchersForKey(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, traceId,
                        result, dispatchers);
        }
        return result;
    }

    @Override
    protected void onRemoved(
        MessageDispatcher dispatcher)
    {
        Iterator<Entry<UnsafeBuffer, MessageDispatcher[]>> entries = dispatchersByKey.entrySet().iterator();
        while (entries.hasNext())
        {
            Entry<UnsafeBuffer, MessageDispatcher[]> entry = entries.next();
            MessageDispatcher[] updated = ArrayUtil.remove(entry.getValue(), dispatcher);
            if (updated.length == 0)
            {
                entries.remove();
            }
            else if (updated != entry.getValue())
            {
                entry.setValue(updated);
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

    private int updateDispatchersForKey(
        int partition,
        long requestOffset,
        long messageOffset,
        DirectBuffer key,
        Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader,
        long timestamp,
        long traceId,
        int result,
        List<MessageDispatcher> newDispatchers)
    {
        buffer.wrap(key);
        MessageDispatcher[] previousDispatchers = dispatchersByKey.get(buffer);
        if (previousDispatchers == null)
        {
            if (!newDispatchers.isEmpty())
            {
                dispatchersByKey.put(makeCopy(buffer),
                        newDispatchers.toArray(new HeadersMessageDispatcher[newDispatchers.size()]));
            }
        }
        else
        {
            boolean updated = false;
            for (MessageDispatcher dispatcher : previousDispatchers)
            {
                if (!newDispatchers.contains(dispatcher))
                {
                    updated = true;
                    // Send tombstone message
                    result |= dispatcher.dispatch(partition, requestOffset, messageOffset,
                                key, supplyHeader, timestamp, traceId, null);
                }
            }
            if (newDispatchers.isEmpty())
            {
                dispatchersByKey.remove(buffer);
            }
            else if (updated || newDispatchers.size() != previousDispatchers.length)

            {
                dispatchersByKey.put(buffer, newDispatchers.toArray(new MessageDispatcher[newDispatchers.size()]));
            }
        }
        return result;
    }

}
