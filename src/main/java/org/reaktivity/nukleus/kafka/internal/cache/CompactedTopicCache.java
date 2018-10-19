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
package org.reaktivity.nukleus.kafka.internal.cache;

import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex.Entry;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class CompactedTopicCache implements TopicCache
{
    private final MessageFW messageRO = new MessageFW();
    private final HeadersFW headersRO = new HeadersFW();
    private final MessageCache messageCache;
    private final PartitionIndex[] indexes;
    private final KeyedMessageIterator keyedMessageIterator = new KeyedMessageIterator();
    private final MessageIterator messageIterator;

    public CompactedTopicCache(
        int partitionCount,
        int deleteRetentionMs,
        MessageCache messageCache)
    {
        this.messageCache = messageCache;
        indexes = new CompactedPartitionIndex[partitionCount];
        for (int i = 0; i < partitionCount; i++)
        {
            indexes[i] = new CompactedPartitionIndex(1000, deleteRetentionMs, messageCache);
        }
        messageIterator = new MessageIterator(partitionCount);
    }

    // For unit tests
    CompactedTopicCache(
        PartitionIndex[] indexes,
        MessageCache messageCache)
    {
        this.indexes = indexes;
        this.messageCache = messageCache;
        messageIterator = new MessageIterator(indexes.length);
    }

    @Override
    public void add(
        int partition,
        long requestOffset,
        long messageStartOffset,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value,
        boolean cacheIfNew)
    {
        indexes[partition].add(requestOffset, messageStartOffset, timestamp, traceId, key, headers, value,
                cacheIfNew);
    }

    @Override
    public Iterator<Message> getMessages(
        Long2LongHashMap fetchOffsets,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers)
    {
        if (fetchKey != null)
        {
            return keyedMessageIterator.reset(fetchOffsets, fetchKey, headers);
        }
        else
        {
            return messageIterator.reset(fetchOffsets, headers);
        }
    }

    @Override
    public void extendNextOffset(
        int partition,
        long requestOffset,
        long lastOffset)
    {
        indexes[partition].extendNextOffset(requestOffset, lastOffset);
    }

    @Override
    public long getOffset(
        int partition,
        OctetsFW key)
    {
        return indexes[partition].getOffset(key);
    }

    @Override
    public long nextOffset(
        int partition)
    {
        return indexes[partition].nextOffset();
    }

    @Override
    public void startOffset(
        int partition,
        long startOffset)
    {
        indexes[partition].startOffset(startOffset);
    }

    private class MessageImpl implements Message
    {
        private int partition;
        private long offset;
        private int messageHandle;

        private MessageImpl wrap(
            int partition,
            long offset,
            int messageHandle)
        {
            this.partition = partition;
            this.offset = offset;
            this.messageHandle = messageHandle;
            return this;
        }

        private MessageImpl wrap(
            int partition,
            Entry entry)
        {
            this.partition = partition;
            this.offset =  entry.offset();
            this.messageHandle = entry.messageHandle();
            return this;
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public int partition()
        {
            return partition;
        }

        @Override
        public MessageFW message()
        {
            return messageCache.get(messageHandle, messageRO);
        }
    }

    final class MessageIterator implements Iterator<Message>
    {
        private final Iterator<Entry>[]  iterators;
        private int partition = -1;
        private final MessageImpl message = new MessageImpl();

        @SuppressWarnings("unchecked")
        MessageIterator(
            int partitionCount)
        {
            iterators = new Iterator[partitionCount];
        }

        Iterator<Message> reset(
            Long2LongHashMap fetchOffsets,
            ListFW<KafkaHeaderFW> headerConditions)
        {
            assert fetchOffsets.size() == iterators.length;

            for (int i=0; i < iterators.length; i++)
            {
                iterators[i] = indexes[i].entries(fetchOffsets.get(i), headerConditions);
            }

            return this;
        }

        @Override
        public boolean hasNext()
        {
            boolean result = false;
            partition = nextPartition(partition);
            for (int i=0; i < iterators.length; i++)
            {
                result = iterators[partition].hasNext();
                if (result)
                {
                    break;
                }
                else
                {
                    partition = nextPartition(partition);
                }
            }
            return result;
        }

        @Override
        public Message next()
        {
            return message.wrap(partition, iterators[partition].next());
        }

        private int nextPartition(int partition)
        {
            int result = ++partition;
            result = result == iterators.length ? 0 : result;
            return result;
        }
    }

    final class KeyedMessageIterator implements Iterator<Message>
    {
        private final MessageImpl message = new MessageImpl();
        private final MessageImpl lastMessage = new MessageImpl();
        private int remainingEntries;

        Iterator<Message> reset(
            Long2LongHashMap fetchOffsets,
            OctetsFW fetchKey,
            ListFW<KafkaHeaderFW> headerConditions)
        {
            int partition = fetchOffsets.keySet().iterator().next().intValue();
            remainingEntries = 1;
            lastMessage.wrap(partition, indexes[partition].nextOffset(), NO_MESSAGE);
            Entry entry = indexes[partition].getEntry(fetchKey);
            if (entry != null)
            {
                message.wrap(partition,  entry.offset(), entry.messageHandle());
                MessageFW messageRO = message.message();
                if (messageRO != null && headersRO.wrap(messageRO.headers()).matches(headerConditions))
                {
                    remainingEntries = 2;
                }
            }
            return this;
        }

        @Override
        public boolean hasNext()
        {
            return remainingEntries > 0;
        }

        @Override
        public Message next()
        {
            Message result;
            switch(remainingEntries)
            {
            case 2:
                result = message;
                break;
            case 1:
                result = lastMessage;
                break;
            default:
                result = null;
            }
            remainingEntries--;
            return result;
        }
    }
}
