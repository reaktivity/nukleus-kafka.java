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

import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.DEBUG;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.DEBUG1;
import static org.reaktivity.nukleus.kafka.internal.cache.TopicCache.NO_OFFSET;

import java.util.Iterator;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.cache.TopicCache;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class TopicMessageDispatcher implements MessageDispatcher, DecoderMessageDispatcher
{
    private final KeyMessageDispatcher[] keys;
    private final HeadersMessageDispatcher headers;
    private final BroadcastMessageDispatcher broadcast;
    private final TopicCache cache;
    private final String topicName;

    private final OctetsFW octetsRO = new OctetsFW();

    private final boolean[] cacheNewMessages;

    protected TopicMessageDispatcher(
        String topicName,
        TopicCache cache,
        int partitionCount)
    {
        this.topicName = topicName;
        this.broadcast = new BroadcastMessageDispatcher(topicName);
        this.cache = cache;
        keys = new KeyMessageDispatcher[partitionCount];
        cacheNewMessages = new boolean[partitionCount];
        BiFunction<String, DirectBuffer, HeaderValueMessageDispatcher> createHeaderValueMessageDispatcher =
                cache.compacted() ? CompactedHeaderValueMessageDispatcher::new : HeaderValueMessageDispatcher::new;
        for (int partition = 0; partition < partitionCount; partition++)
        {
            keys[partition] = new KeyMessageDispatcher(topicName, createHeaderValueMessageDispatcher);
        }
        headers = new HeadersMessageDispatcher(topicName, createHeaderValueMessageDispatcher);
    }

    @Override
    public void startOffset(
        int partition,
        long startOffset)
    {
        cache.startOffset(partition, startOffset);
    }

    @Override
    public void adjustOffset(
        int partition,
        long oldOffset,
        long newOffset)
    {
        broadcast.adjustOffset(partition, oldOffset, newOffset);
        for (int i=0; i < keys.length; i++)
        {
            keys[i].adjustOffset(partition, oldOffset, newOffset);
        }
        headers.adjustOffset(partition, oldOffset, newOffset);
    }

    @Override
    public void detach(
        boolean reattach)
    {
        broadcast.detach(reattach);
        for (int i=0; i < keys.length; i++)
        {
            keys[i].detach(reattach);
        }
        headers.detach(reattach);
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
        if (DEBUG1)
        {
            System.out.format("TMD.dispatch.3: topic=%s partition=%d requestOffset=%d messageOffset=%d\n",
                    topicName,
                    partition, requestOffset, messageOffset);
        }
        if (cache.compacted() && key == null)
        {
            return 0;
        }
        int result = dispatch(partition, requestOffset, messageOffset, key, headers.headerSupplier(), timestamp,
                traceId, value);

        if (messageOffset + 1 == highWatermark)
        {
            // Caught up to live stream, enable pro-active message caching
            if (!cacheNewMessages[partition])
            {
                cacheNewMessages[partition] = true;
            }
        }

        if (MessageDispatcher.matched(result))
        {
            cache.add(partition, requestOffset, messageOffset, timestamp, traceId, key, headers, value,
                    cacheNewMessages[partition]);
        }

        if (DEBUG1)
        {
            System.out.format("TMD.dispatch.4: topic=%s partition=%d requestOffset=%d messageOffset=%d 1.result=%d\n",
                    topicName,
                    partition, requestOffset, messageOffset, result);
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
            if (DEBUG1)
            {
                System.out.format("TMD.dispatch.1: topic=%s partition=%d requestOffset=%d messageOffset=%d 1.result=%d\n",
                        topicName,
                        partition, requestOffset, messageOffset, result);
            }
            if (key != null)
            {
                KeyMessageDispatcher keyDispatcher = keys[partition];
                result |= keyDispatcher.dispatch(partition, requestOffset, messageOffset,
                                                   key, supplyHeader, timestamp, traceId, value);
                // detect historical message stream
                long highestOffset = cache.nextOffset(partition);
                if (MessageDispatcher.delivered(result) && messageOffset < highestOffset)
                {
                    long offset = cache.getOffset(partition, asOctets(key));

                    // fast-forward to live stream after observing most recent cached offset for message key
                    if (offset != NO_OFFSET && offset == messageOffset)
                    {
                        keyDispatcher.flush(partition, requestOffset, highestOffset, key);
                    }
                }
            }
            result |= headers.dispatch(partition, requestOffset, messageOffset, key, supplyHeader, timestamp, traceId, value);
        }
        if (DEBUG1)
        {
            System.out.format("TMD.dispatch.2: topic=%s partition=%d requestOffset=%d messageOffset=%d 2.result=%d\n",
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
        broadcast.flush(partition, requestOffset, lastOffset);
        keys[partition].flush(partition, requestOffset, lastOffset);
        headers.flush(partition, requestOffset, lastOffset);
        cache.extendNextOffset(partition, requestOffset, lastOffset);
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
            long expectedOffsetForKey = cache.getOffset(partition, asOctets(key));
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
