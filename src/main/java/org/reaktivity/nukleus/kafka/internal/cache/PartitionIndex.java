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
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public interface PartitionIndex
{
    public interface Entry
    {
        long offset();

        int messageHandle();
    }

    /**
     * Adds the message details to the index, and conditionally caches the whole message
     * @param cacheIfNew   If true, the message is always cached.
     *                     If false, the message is cached only if it is historical.
     */
    void add(
        long requestOffset,
        long messageStartOffset,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value,
        boolean cacheIfNew);

    Iterator<Entry> entries(
        long requestOffset,
        ListFW<KafkaHeaderFW> headerConditions);

    long getOffset(
        OctetsFW key);

    long nextOffset();

    void extendNextOffset(
        long requestOffset,
        long lastOffset);

    void startOffset(
        long startOffset);

}