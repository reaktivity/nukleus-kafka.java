/**
 * Copyright 2016-2020 The Reaktivity Project
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

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

/**
 * A source of messages for a topic
 */
public interface ImmutableTopicCache
{
    public interface MessageRef
    {
        /*
         * @return the offset of this message
         */
        long offset();

        int partition();

        /*
         * @return The message,
         *         OR null if there are no more messages (we are at live offset) or this message is not
         *         available in the cache; in both cases offset() should be used to fetch further
         *         messages from Kafka.
         */
        MessageFW message();
    }

    Iterator<MessageRef> getMessages(
        Long2LongHashMap fetchOffsets,
        OctetsFW fetchKey,
        ArrayFW<KafkaHeaderFW> headers);

    MessageRef getMessage(
        int partition,
        long offset);

    /**
     * Reports if the cache contains any messages matching the key and headers starting at the given offsets.
     * This is on a best effort basis, depending on performance cost.
     * @param fetchOffsets
     * @param fetchKey
     * @param headers
     * @return true if the cache <i>may</i> contain matching messages
     *         false if the cache definitively does not contain any matching messages
     */
    boolean hasMessages(
        Long2LongHashMap fetchOffsets,
        OctetsFW fetchKey,
        ArrayFW<KafkaHeaderFW> headers);
}
