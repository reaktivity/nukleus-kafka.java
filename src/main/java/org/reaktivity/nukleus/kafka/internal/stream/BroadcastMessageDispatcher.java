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
import java.util.List;
import java.util.function.Function;

import org.agrona.DirectBuffer;

public class BroadcastMessageDispatcher implements MessageDispatcher
{
    private final List<MessageDispatcher> dispatchers = new ArrayList<MessageDispatcher>();

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
        int result = 0;
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            result += dispatcher.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, value);
        }
        return result;
    }

    @Override
    public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
    {
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            dispatcher.flush(partition, requestOffset, lastOffset);
        }
    }

    public void add(MessageDispatcher dispatcher)
    {
         dispatchers.add(dispatcher);
    }

    public boolean remove(MessageDispatcher dispatcher)
    {
        return dispatchers.remove(dispatcher);
    }

    public boolean isEmpty()
    {
        return dispatchers.isEmpty();
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s)", this.getClass().getSimpleName(), dispatchers);
    }

}
