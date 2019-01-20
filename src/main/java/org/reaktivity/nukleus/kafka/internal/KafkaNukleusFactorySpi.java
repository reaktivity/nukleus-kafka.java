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
package org.reaktivity.nukleus.kafka.internal;

import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.NukleusFactorySpi;
import org.reaktivity.nukleus.kafka.internal.memory.MemoryManager;

public final class KafkaNukleusFactorySpi implements NukleusFactorySpi
{
    public static final String MESSAGE_CACHE_BUFFER_ACQUIRES = "kafka.message.cache.buffer.acquires";

    static final MemoryManager OUT_OF_SPACE_MEMORY_MANAGER = new MemoryManager()
    {

        @Override
        public long acquire(
            int capacity)
        {
            return -1;
        }

        @Override
        public long resolve(
            long address)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void release(
            long address,
            int capacity)
        {
            throw new UnsupportedOperationException();
        }

    };

    @Override
    public String name()
    {
        return KafkaNukleus.NAME;
    }

    @Override
    public KafkaNukleus create(
        Configuration config)
    {
        return new KafkaNukleus(new KafkaConfiguration(config));
    }
}
