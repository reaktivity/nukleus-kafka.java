/**
 * Copyright 2016-2019 The Reaktivity Project
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

import java.util.Arrays;

import org.agrona.DirectBuffer;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.memory.MemoryManager;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class DefaultMessageCache implements MessageCache
{
    private static final long NO_ADDRESS = -1L;
    private static final long EVICTED = -2L;
    private static final int LRU_SCAN_SIZE = 10;

    private final MessageFW.Builder messageRW = new MessageFW.Builder();
    private final OctetsFW keyRO = new OctetsFW();
    private final OctetsFW valueRO = new OctetsFW();

    private final MemoryManager memoryManager;
    private final UnsafeBuffer buffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final LongArrayList addresses = new LongArrayList(1024, NO_ADDRESS);
    private final LongArrayList accessTimes = new LongArrayList(1024, NO_ADDRESS);

    // Result of scanning for LRU entries
    private final int[] lruHandles;
    private final long[] lruTimes;
    private int lruPosition;

    private long time = 0;
    private int entries = 0;

    public DefaultMessageCache(
        MemoryManager memoryManager)
    {
        this.memoryManager = memoryManager;
        lruHandles = new int[LRU_SCAN_SIZE];
        lruTimes = new long[lruHandles.length];
        lruPosition = lruHandles.length;
    }

    @Override
    public MessageFW get(
        int messageHandle,
        MessageFW message)
    {
        MessageFW result = null;
        long address;
        if (messageHandle != NO_MESSAGE &&
            (address = addresses.getLong(messageHandle)) >= 0L)
        {
            long memoryAddress = memoryManager.resolve(address);
            buffer.wrap(memoryAddress, Integer.BYTES);
            int size = buffer.getInt(0) + Integer.BYTES;
            buffer.wrap(memoryAddress, size);
            accessTimes.setLong(messageHandle,  time++);
            clearLruEntries();
            result =  message.wrap(buffer, Integer.BYTES, size);
        }
        return result;
    }

    @Override
    public int put(
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value)
    {
        int index = nextFreeIndex();
        return set(index, timestamp, traceId, key, headers, value);
    }

    @Override
    public int release(
        int messageHandle)
    {
        int bytesReleased = releaseMemory(messageHandle);
        addresses.setLong(messageHandle, NO_ADDRESS);
        accessTimes.setLong(messageHandle, Long.MAX_VALUE);
        entries--;
        return bytesReleased;
    }

    @Override
    public int replace(
        int messageHandle,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value)
    {
        int result;
        if (messageHandle == NO_MESSAGE)
        {
            result = put(timestamp, traceId, key, headers, value);
        }
        else
        {
            release(messageHandle);
            result = set(messageHandle, timestamp, traceId, key, headers, value);
        }
        return result;
    }

    private void clearLruEntries()
    {
        lruPosition = lruHandles.length;
    }

    private void evict(
        final int size)
    {
        int released = 0;
        int lruIndex;
        while (released  < size && (lruIndex = getLruHandle()) != NO_MESSAGE)
        {
            released += releaseMemory(lruIndex);
            addresses.set(lruIndex, EVICTED);
            accessTimes.setLong(lruIndex, Long.MAX_VALUE);
        }
    }

    private void findLruEntries()
    {
        Arrays.fill(lruHandles, NO_MESSAGE);
        Arrays.fill(lruTimes, Long.MAX_VALUE);
        for (int i = 0; i < entries; i++)
        {
            final long time = accessTimes.getLong(i);
            for (int j = 0; j < lruTimes.length; j++)
            {
                if (time < lruTimes[j])
                {
                    System.arraycopy(lruTimes, j, lruTimes, j + 1, lruTimes.length - j - 1);
                    System.arraycopy(lruHandles, j, lruHandles, j + 1, lruHandles.length - j - 1);
                    lruTimes[j] = time;
                    lruHandles[j] = i;
                    break;
                }
            }
        }
        lruPosition = 0;
    }

    private int getLruHandle()
    {
        if (lruPosition >= lruHandles.length)
        {
            findLruEntries();
            lruPosition = 0;
        }
        int result = lruHandles[lruPosition];
        if (result != NO_MESSAGE)
        {
            lruPosition++;
        }
        return result;
    }

    private int nextFreeIndex()
    {
        int index = addresses.size();
        if (entries == index)
        {
            addresses.addLong(NO_ADDRESS);
            accessTimes.addLong(Long.MAX_VALUE);
        }
        else
        {
            for (index = 0; addresses.getLong(index) != NO_ADDRESS; index++)
            {

            }
        }
        return index;
    }

    private int releaseMemory(
        int messageHandle)
    {
        int bytesReleased = 0;
        long address = addresses.getLong(messageHandle);
        if (address >= 0)
        {
            long memoryAddress = memoryManager.resolve(address);
            buffer.wrap(memoryAddress, Integer.BYTES);
            bytesReleased = buffer.getInt(0) + Integer.BYTES;
            memoryManager.release(address, bytesReleased);
            addresses.setLong(messageHandle, NO_ADDRESS);
        }
        return bytesReleased;
    }

    private int set(
        int index,
        long timestamp,
        long traceId,
        DirectBuffer key,
        HeadersFW headers,
        DirectBuffer value)
    {
        int result = NO_MESSAGE;
        final int messageSize =
            MessageFW.FIELD_OFFSET_KEY +
            (key == null ? 0 : key.capacity()) +
            Integer.BYTES +
            headers.sizeof() +
            Integer.BYTES +
            (value == null ? 0 : value.capacity());

        final int size = messageSize + Integer.BYTES;
        long address = memoryManager.acquire(size);
        if (address == OUT_OF_MEMORY)
        {
            evict(size);
            address = memoryManager.acquire(size);
        }
        if (address != OUT_OF_MEMORY)
        {
            buffer.wrap(memoryManager.resolve(address), size);
            buffer.putInt(0, messageSize);
            OctetsFW keyFW = key == null ? null : keyRO.wrap(key, 0, key.capacity());
            OctetsFW valueFW = value == null ? null : valueRO.wrap(value,  0, value.capacity());
            messageRW.wrap(buffer, Integer.BYTES, size)
                     .timestamp(timestamp)
                     .traceId(traceId)
                     .key(keyFW)
                     .headers(headers.buffer(), headers.offset(), headers.sizeof())
                     .value(valueFW)
                     .build();
            addresses.set(index,  address);
            accessTimes.setLong(index, time++);
            result = index;
            entries++;
        }
        return result;
    }

}
