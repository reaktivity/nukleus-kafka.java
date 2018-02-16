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

import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;

public class TopicMessageDispatcher implements MessageDispatcher
{
    private final KeyMessageDispatcher keys = new KeyMessageDispatcher();
    private final HeadersMessageDispatcher headers = new HeadersMessageDispatcher();
    private final BroadcastMessageDispatcher broadcast = new BroadcastMessageDispatcher();

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
        result += broadcast.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, value);
        if (key != null)
        {
            result += keys.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, value);
        }
        result += headers.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, value);
        return result;
    }

    @Override
    public void flush(
            int partition,
            long requestOffset,
            long lastOffset)
    {
        broadcast.flush(partition, requestOffset, lastOffset);
        keys.flush(partition, requestOffset, lastOffset);
        headers.flush(partition, requestOffset, lastOffset);
    }

    public void add(OctetsFW fetchKey,
                    ListFW<KafkaHeaderFW> headers,
                    MessageDispatcher dispatcher)
    {
         if (fetchKey != null)
         {
             keys.add(fetchKey, headers, dispatcher);
         }
         else if (headers != null && !headers.isEmpty())
         {
             this.headers.add(headers, 0, dispatcher);
         }
         else
         {
             broadcast.add(dispatcher);
         }
    }

    public boolean remove(OctetsFW fetchKey,
                       ListFW<KafkaHeaderFW> headers,
                       MessageDispatcher dispatcher)
      {
           boolean result = false;
           if (fetchKey != null)
           {
               result = keys.remove(fetchKey, headers, dispatcher);
           }
           else if (headers != null && headers.isEmpty())
           {
               result = this.headers.remove(headers, 0, dispatcher);
           }
           else
           {
               result = broadcast.remove(dispatcher);
           }
           return result;
      }

    public boolean isEmpty()
    {
        return keys.isEmpty() && headers.isEmpty() && broadcast.isEmpty();
    }

}
