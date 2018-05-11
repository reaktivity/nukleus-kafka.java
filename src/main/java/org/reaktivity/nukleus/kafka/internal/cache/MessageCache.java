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

import static org.reaktivity.nukleus.kafka.internal.memory.MemoryManager.OUT_OF_MEMORY;
import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.EMPTY_BYTE_ARRAY;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.memory.MemoryManager;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;

public class MessageCache
{
    private final MemoryManager memoryManager;
    private final UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    MessageCache(MemoryManager memoryManager)
    {
        this.memoryManager = memoryManager;
    }

    public long cacheMessage(
        DirectBuffer key,
        HeadersFW headers,
        long timestamp,
        long traceId,
        DirectBuffer value)
    {
        final int messageSize =
            MessageFW.FIELD_OFFSET_KEY +
            (key == null ? 0 : key.capacity()) +
            Integer.BYTES +
            (headers == null ? 0 : headers.sizeOf()) +
            (value == null ? 0 : value.capacity());

        final int size = messageSize + Integer.BYTES;
        long address = memoryManager.acquire(size);
        if (address == OUT_OF_MEMORY)
        {
            int released = 0;
            long lruAddress = getLRUAddress();
            while (released  < size && (lruAddress = getLRUAddress()) != OUT_OF_MEMORY)
            {
                released += release(lruAddress);
            }
            address = memoryManager.acquire(size);
        }
        if (address != OUT_OF_MEMORY)
        {
            buffer.wrap(memoryManager.resolve(address), Integer.BYTES);
            buffer.putInt(0, size);
        }
        return address;
    }

    public MessageFW get(
        long address,
        MessageFW.Builder builder)
    {
        long memoryAddress = memoryManager.resolve(address);
        buffer.wrap(memoryAddress, Integer.BYTES);
        int size = buffer.getInt(0);
        buffer.wrap(memoryAddress, size);
        return builder.wrap(buffer, 0, size).build();
    }

    private long getLRUAddress()
    {
        // TODO: implement
        return OUT_OF_MEMORY;
    }

    private int release(
        long address)
    {
        long memoryAddress = memoryManager.resolve(address);
        buffer.wrap(memoryAddress, Integer.BYTES);
        int size = buffer.getInt(0) + Integer.BYTES;
        memoryManager.release(memoryAddress, size);
        return size;
    }

}
