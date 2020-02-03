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
package org.reaktivity.nukleus.kafka.internal.memory;

import static java.lang.Long.highestOneBit;
import static java.lang.Long.numberOfTrailingZeros;
import static org.agrona.BitUtil.findNextPositivePowerOfTwo;
import static org.reaktivity.nukleus.kafka.internal.memory.BTreeFW.EMPTY;
import static org.reaktivity.nukleus.kafka.internal.memory.BTreeFW.FULL;
import static org.reaktivity.nukleus.kafka.internal.memory.BTreeFW.SPLIT;
import static org.reaktivity.nukleus.kafka.internal.memory.MemoryLayout.BTREE_OFFSET;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.reaktivity.nukleus.kafka.internal.KafkaCounters;

// NOTE, order 0 is largest in terms of size
public class DefaultMemoryManager implements MemoryManager
{
    private final BTreeFW btreeRO;

    private final int blockSizeShift;
    private final int maximumOrder;

    private final MutableDirectBuffer[] memoryBuffers;
    private final int maximumBlockSize;
    private final int addressShift;
    private final long addressMask;
    private final AtomicBuffer metadataBuffer;
    private final KafkaCounters counters;


    public DefaultMemoryManager(
        MemoryLayout memoryLayout,
        KafkaCounters counters)
    {
        final long minimumBlockSize = memoryLayout.minimumBlockSize();
        final long capacity = memoryLayout.capacity();

        this.memoryBuffers = memoryLayout.memoryBuffers();
        this.metadataBuffer = memoryLayout.metadataBuffer();
        this.blockSizeShift = numberOfTrailingZeros(minimumBlockSize);
        this.maximumOrder = numberOfTrailingZeros(capacity) - numberOfTrailingZeros(minimumBlockSize);
        this.btreeRO = new BTreeFW(maximumOrder).wrap(metadataBuffer, BTREE_OFFSET, metadataBuffer.capacity() - BTREE_OFFSET);
        maximumBlockSize = memoryBuffers[0].capacity();
        addressShift = Integer.numberOfTrailingZeros(maximumBlockSize);
        addressMask = maximumBlockSize - 1;
        this.counters = counters;
    }

    @Override
    public long resolve(
        long address)
    {
        int index = (int) (address >> addressShift);
        MutableDirectBuffer memoryBuffer = memoryBuffers[index];
        address &= addressMask;
        return memoryBuffer.addressOffset() + address;
    }

    @Override
    public long acquire(
        final int capacity)
    {
        if (capacity > this.maximumBlockSize)
        {
            return -1;
        }

        final int allocationSize = Math.max(findNextPositivePowerOfTwo(capacity), 1 << blockSizeShift);
        final int allocationOrder = numberOfTrailingZeros(allocationSize >> blockSizeShift);

        final BTreeFW node = btreeRO.walk(0);

        while (node.order() != allocationOrder || node.flag(SPLIT) || node.flag(FULL))
        {
            if (node.order() < allocationOrder || node.flag(FULL))
            {
                while (node.isRightChild())
                {
                    node.walk(node.parentIndex());
                }

                if (node.isLeftChild())
                {
                    node.walk(node.siblingIndex());
                }
                else
                {
                    break; // root
                }
            }
            else
            {
                node.walk(node.leftIndex());
            }
        }

        if (node.flag(FULL) || node.flag(SPLIT))
        {
            return -1L;
        }
        assert node.order() == allocationOrder;

        node.set(FULL);

        final int nodeIndex = node.index();
        final int nodeOrder = node.order();

        while (node.order() < maximumOrder)
        {
            node.walk(node.parentIndex());

            if (node.flag(node.leftIndex(), FULL) && node.flag(node.rightIndex(), FULL))
            {
                node.set(FULL);
            }
            else
            {
                if (node.flag(SPLIT))
                {
                    break;
                }

                node.set(SPLIT);
            }
        }

        counters.cacheInUse.accept(allocationSize);

        return ((nodeIndex + 1) & ~highestOneBit(nodeIndex + 1)) << blockSizeShift << nodeOrder;
    }

    @Override
    public void release(
        long offset,
        int capacity)
    {
        final int allocationSize = Math.max(findNextPositivePowerOfTwo(capacity), 1 << blockSizeShift);
        final int nodeOrder = numberOfTrailingZeros(allocationSize >> blockSizeShift);
        final int nodeIndex = (((int) (offset >> nodeOrder >> blockSizeShift)) | (1 << (maximumOrder - nodeOrder))) - 1;

        final BTreeFW node = btreeRO.walk(nodeIndex);
        for (;; node.walk(node.parentIndex()))
        {
            node.clear(FULL);
            if (node.order() == 0 || (node.flags(node.leftIndex()) == EMPTY && node.flags(node.rightIndex()) == EMPTY))
            {
                node.clear(SPLIT);
            }
            else
            {
                node.set(SPLIT);
            }

            if (node.index() == 0)
            {
                break;
            }
        }

        counters.cacheInUse.accept(-allocationSize);
    }

    public boolean released()
    {
        return btreeRO.walk(0).flags() == EMPTY;
    }
}
