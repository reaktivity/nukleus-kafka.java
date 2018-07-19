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

import java.util.Iterator;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex.Entry;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class TopicMessageDispatcher implements MessageDispatcher, DecoderMessageDispatcher
{
    private final KeyMessageDispatcher[] keys;
    private final HeadersMessageDispatcher headers;
    private final BroadcastMessageDispatcher broadcast = new BroadcastMessageDispatcher();
    private final PartitionIndex[] indexes;

    private final OctetsFW octetsRO = new OctetsFW();

    private final boolean[] cacheNewMessages;

    protected TopicMessageDispatcher(
        PartitionIndex[] indexes,
        Function<DirectBuffer, HeaderValueMessageDispatcher> createHeaderValueMessageDispatcher)
    {
        this.indexes = indexes;
        keys = new KeyMessageDispatcher[indexes.length];
        cacheNewMessages = new boolean[indexes.length];
        for (int partition = 0; partition < indexes.length; partition++)
        {
            keys[partition] = new KeyMessageDispatcher(createHeaderValueMessageDispatcher);
        }
        headers = new HeadersMessageDispatcher(createHeaderValueMessageDispatcher);
    }

    @Override
    public void detach()
    {
        broadcast.detach();
        for (int i=0; i < keys.length; i++)
        {
            keys[i].detach();
        }
        headers.detach();
    }

    @Override
    public int dispatch(
        int partition,
        long requestOffset,
        long messageOffset,
        long highWatermark,
        DirectBuffer key,
        HeadersFW headers,
        long timestamp,
        long traceId,
        DirectBuffer value)
    {
        int result = dispatch(partition, requestOffset, messageOffset, key, headers.headerSupplier(), timestamp,
                traceId, value);
        if (messageOffset + 1 == highWatermark)
        {
            // Caught up to live stream, enable proactive message caching
            cacheNewMessages[partition] = true;
        }
        if (MessageDispatcher.matched(result))
        {
            indexes[partition].add(requestOffset, messageOffset, timestamp, traceId, key, headers, value,
                    cacheNewMessages[partition]);
        }
        return result;
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
        if (shouldDispatch(partition, requestOffset, messageOffset, key))
        {
            result |= broadcast.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, traceId, value);
            if (key != null)
            {
                KeyMessageDispatcher keyDispatcher = keys[partition];
                result |= keyDispatcher.dispatch(partition, requestOffset, messageOffset,
                                                   key, supplyHeader, timestamp, traceId, value);
                // detect historical message stream
                long highestOffset = indexes[partition].nextOffset();
                if (MessageDispatcher.delivered(result) && messageOffset < highestOffset)
                {
                    Entry entry = getEntry(partition, requestOffset, asOctets(key));

                    // fast-forward to live stream after observing most recent cached offset for message key
                    if (entry != null  && entry.offset() == messageOffset)
                    {
                        keyDispatcher.flush(partition, requestOffset, highestOffset, key);
                    }
                }
            }
            result |= headers.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, traceId, value);
        }
        return result;
    }

    @Override
    public void flush(
        int partition,
        long requestOffset,
        long lastOffset)
    {
        broadcast.flush(partition, requestOffset, lastOffset);
        keys[partition].flush(partition, requestOffset, lastOffset);
        headers.flush(partition, requestOffset, lastOffset);
        indexes[partition].extendNextOffset(requestOffset, lastOffset);
    }

    public void add(
        OctetsFW fetchKey,
        int fetchKeyPartition,
        Iterator<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher)
    {
         if (fetchKey != null)
         {
             keys[fetchKeyPartition].add(fetchKey, headers, dispatcher);
         }
         else if (headers.hasNext())
         {
             this.headers.add(headers, dispatcher);
         }
         else
         {
             broadcast.add(dispatcher);
         }
    }

    public boolean remove(
        OctetsFW fetchKey,
        int fetchKeyPartition,
        Iterator<KafkaHeaderFW> headers,
        MessageDispatcher dispatcher)
      {
           boolean result = false;
           if (fetchKey != null)
           {
               result = keys[fetchKeyPartition].remove(fetchKey, headers, dispatcher);
           }
           else if (headers.hasNext())
           {
               result = this.headers.remove(headers, dispatcher);
           }
           else
           {
               result = broadcast.remove(dispatcher);
           }
           return result;
      }

    public boolean isEmpty()
    {
        boolean empty = true;
        for (KeyMessageDispatcher keys : keys)
        {
            empty = empty && keys.isEmpty();
        }
        return empty && headers.isEmpty() && broadcast.isEmpty();
    }

    public Entry getEntry(
        int partition,
        long requestOffset,
        OctetsFW key)
    {
        return indexes[partition].getEntry(requestOffset, key);
    }

    public Iterator<Entry> entries(
        int partition,
        long requestOffset)
    {
        return indexes[partition].entries(requestOffset);
    }

    public long nextOffset(
        int partition)
    {
        return indexes[partition].nextOffset();
    }

    public void enableProactiveMessageCaching()
    {
        for (int i=0; i < cacheNewMessages.length; i++)
        {
            cacheNewMessages[i] = true;
        }
    }

    private boolean shouldDispatch(
        int partition,
        long requestOffset,
        long messageStartOffset,
        DirectBuffer key)
    {
        boolean result = true;
        if (key != null)
        {
            Entry entry = getEntry(partition, requestOffset, asOctets(key));
            long expectedOffsetForKey = entry.offset();
            result = messageStartOffset >= expectedOffsetForKey;
        }
        return result;
    }

    private OctetsFW asOctets(
        DirectBuffer key)
    {
        return octetsRO.wrap(key, 0, key.capacity());
    }

}
