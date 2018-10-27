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
package org.reaktivity.nukleus.kafka.internal.cache;

import java.util.Collections;
import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

/**
 * A cache of messages for a topic
 */
public final class DefaultTopicCache implements TopicCache
{
    public static final TopicCache INSTANCE = new DefaultTopicCache();

    public final NoMessage noMessage = new NoMessage();

    private static class NoMessage implements Message
    {
        private int partition;
        private long offset;

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
            return null;
        }

        private Message wrap(
            int partition,
            long offset)
        {
            this.partition = partition;
            this.offset = offset;
            return this;
        }
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
        boolean cacheNewMessages)
    {
    }

    @Override
    public Iterator<Message> getMessages(
        Long2LongHashMap fetchOffsets,
        OctetsFW fetchKey,
        ListFW<KafkaHeaderFW> headers)
    {
        return Collections.emptyIterator();
    }

    @Override
    public Message getMessage(
        int partition,
        long offset)
    {
        return noMessage.wrap(partition, offset);
    }

    @Override
    public void extendNextOffset(
        int partition,
        long requestOffset,
        long lastOffset)
    {
    }

    @Override
    public long getOffset(
        int partition,
        OctetsFW key)
    {
        return NO_OFFSET;
    }

    @Override
    public long nextOffset(
        int partition)
    {
        return 0L;
    }

    @Override
    public void startOffset(
        int partition,
        long startOffset)
    {
    }
}
