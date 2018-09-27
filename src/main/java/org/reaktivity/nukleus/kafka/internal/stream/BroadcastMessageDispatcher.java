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
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.agrona.DirectBuffer;

import static org.reaktivity.nukleus.kafka.internal.stream.HeadersMessageDispatcher.NOOP;

public class BroadcastMessageDispatcher implements MessageDispatcher
{
    private final List<MessageDispatcher> dispatchers = new ArrayList<MessageDispatcher>();

    private boolean inIteration;
    private boolean noopDispatchers;

    @Override
    public void adjustOffset(
        int partition,
        long oldOffset,
        long newOffset)
    {
        //  Avoid iterator allocation
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
        inIteration = true;
        //  Avoid iterator allocation
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            dispatcher.detach(reattach);
        }
        inIteration = false;

        removeNoopDispatchers();
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
        inIteration = true;
        int result = 0;
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            result |= dispatcher.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, traceId, value);
        }
        inIteration = false;

        removeNoopDispatchers();
        return result;
    }

    @Override
    public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
    {
        inIteration = true;
        for (int i = 0; i < dispatchers.size(); i++)
        {
            MessageDispatcher dispatcher = dispatchers.get(i);
            dispatcher.flush(partition, requestOffset, lastOffset);
        }
        inIteration = false;

        removeNoopDispatchers();
    }

    public void add(MessageDispatcher dispatcher)
    {
         dispatchers.add(dispatcher);
    }

    public boolean remove(MessageDispatcher dispatcher)
    {
        int index = dispatchers.indexOf(dispatcher);
        if (index != -1)
        {
            if (inIteration)
            {
                dispatchers.set(index, NOOP);
                noopDispatchers = true;
            }
            else
            {
                dispatchers.remove(index);
            }
        }
        return index != -1;
    }

    public boolean isEmpty()
    {
        return dispatchers.isEmpty();
    }

    private void removeNoopDispatchers()
    {
        if (noopDispatchers)
        {
            noopDispatchers = false;
            dispatchers.removeIf(e -> e == NOOP);
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s)", this.getClass().getSimpleName(), dispatchers);
    }

}
