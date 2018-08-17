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
package org.reaktivity.nukleus.kafka.internal.memory;

import static java.lang.Long.numberOfTrailingZeros;
import static java.lang.Math.max;
import static java.lang.String.format;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;
import static org.agrona.IoUtil.createEmptyFile;
import static org.agrona.IoUtil.mapExistingFile;
import static org.agrona.IoUtil.unmap;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class MemoryLayout extends Layout
{
    public static final int BITS_PER_BYTE_SHIFT = numberOfTrailingZeros(Byte.SIZE);

    public static final int BITS_PER_BTREE_NODE_SHIFT = 1;
    public static final int BITS_PER_BTREE_NODE = 1 << BITS_PER_BTREE_NODE_SHIFT;
    public static final int MASK_PER_BTREE_NODE = (1 << BITS_PER_BTREE_NODE) - 1;

    public static final int MINIMUM_BLOCK_SIZE_OFFSET = 0;
    public static final int MINIMUM_BLOCK_SIZE_SIZE = Integer.BYTES;
    public static final int MAXIMUM_BLOCK_SIZE_OFFSET = MINIMUM_BLOCK_SIZE_OFFSET + MINIMUM_BLOCK_SIZE_SIZE;
    public static final int MAXIMUM_BLOCK_SIZE_SIZE = Long.BYTES;
    public static final int BTREE_OFFSET = MAXIMUM_BLOCK_SIZE_OFFSET + MAXIMUM_BLOCK_SIZE_SIZE;

    public static final long MAX_MAPPABLE_BYTES = Integer.MAX_VALUE;
    public static final int ONE_GB = 0x40000000;

    private final AtomicBuffer metadataBuffer;
    private final MutableDirectBuffer[] memoryBuffers;

    private MemoryLayout(
        AtomicBuffer metadataBuffer,
        MutableDirectBuffer[] memoryBuffers)
    {
        this.metadataBuffer = metadataBuffer;
        this.memoryBuffers = memoryBuffers;
    }

    @Override
    public void close()
    {
        unmap(metadataBuffer.byteBuffer());
        for (MutableDirectBuffer memoryBuffer : memoryBuffers)
        {
            unmap(memoryBuffer.byteBuffer());
        }
    }

    public AtomicBuffer metadataBuffer()
    {
        return metadataBuffer;
    }

    public MutableDirectBuffer[] memoryBuffers()
    {
        return memoryBuffers;
    }

    public int minimumBlockSize()
    {
        return metadataBuffer.getInt(MINIMUM_BLOCK_SIZE_OFFSET);
    }

    public long maximumBlockSize()
    {
        return metadataBuffer.getLong(MAXIMUM_BLOCK_SIZE_OFFSET);
    }

    public static final class Builder extends Layout.Builder<MemoryLayout>
    {
        private Path path;
        private int minimumBlockSize;
        private long maximumBlockSize;
        private boolean create;

        public Builder path(
            Path path)
        {
            this.path = path;
            return this;
        }

        public Builder create(
            boolean create)
        {
            this.create = create;
            return this;
        }

        public Builder minimumBlockSize(
            int minimumBlockSize)
        {
            if (!isPowerOfTwo(minimumBlockSize))
            {
                throw new IllegalArgumentException("minimum block size MUST be a power of 2");
            }

            this.minimumBlockSize = minimumBlockSize;
            return this;
        }

        public Builder maximumBlockSize(
            long maximumBlockSize)
        {
            if (!isPowerOfTwo(maximumBlockSize))
            {
                throw new IllegalArgumentException("maximum block size MUST be a power of 2");
            }

            if (maximumBlockSize >> Integer.numberOfTrailingZeros(ONE_GB) > Integer.MAX_VALUE)
            {
                throw new IllegalStateException("capacity too large, number of 1GB buffers would exceed Integer.MAX_VALUE");
            }

            this.maximumBlockSize = maximumBlockSize;
            return this;
        }

        @Override
        public MemoryLayout build()
        {
            final File memory = path.toFile();
            long metadataSize;
            long metadataSizeAligned;

            if (create)
            {
                metadataSize = BTREE_OFFSET + sizeofBTree(minimumBlockSize, maximumBlockSize);
                metadataSizeAligned = align(metadataSize, CACHE_LINE_LENGTH);
                if (metadataSizeAligned > MAX_MAPPABLE_BYTES)
                {
                    throw new IllegalStateException(format(
                            "BTree size %d exceeds ONE_GB, difference between minimum and maximum block size is too great",
                            metadataSizeAligned));
                }
                CloseHelper.close(createEmptyFile(memory, metadataSizeAligned + maximumBlockSize));
            }
            else
            {
                final MappedByteBuffer mappedBootstrap = mapExistingFile(memory, "bootstrap", 0, BTREE_OFFSET);
                final DirectBuffer bootstrapBuffer = new UnsafeBuffer(mappedBootstrap);
                final long minimumBlockSize = bootstrapBuffer.getLong(MINIMUM_BLOCK_SIZE_OFFSET);
                final long maximumBlockSize = bootstrapBuffer.getLong(MAXIMUM_BLOCK_SIZE_OFFSET);
                metadataSize = BTREE_OFFSET + sizeofBTree(minimumBlockSize, maximumBlockSize);
                metadataSizeAligned = align(metadataSize, CACHE_LINE_LENGTH);
                unmap(mappedBootstrap);
            }

            final MappedByteBuffer mappedMetadata = mapExistingFile(memory, "metadata", 0, metadataSizeAligned);
            final AtomicBuffer metadataBuffer = new UnsafeBuffer(mappedMetadata, 0, (int) metadataSize);
            metadataBuffer.putInt(MINIMUM_BLOCK_SIZE_OFFSET, minimumBlockSize);
            metadataBuffer.putLong(MAXIMUM_BLOCK_SIZE_OFFSET, maximumBlockSize);

            final MappedByteBuffer[] mappedMemoryBuffers;
            long start = metadataSizeAligned;

            if (maximumBlockSize <= ONE_GB)
            {
                mappedMemoryBuffers = new MappedByteBuffer[1];
                mappedMemoryBuffers[0] = mapExistingFile(memory, "memory", start, maximumBlockSize);
            }
            else
            {
                int buffersNeeded = (int) (maximumBlockSize >> Integer.numberOfTrailingZeros(ONE_GB));
                mappedMemoryBuffers = new MappedByteBuffer[buffersNeeded];
                for (int i = 0; i < buffersNeeded; i++)
                {
                    mappedMemoryBuffers[i] = mapExistingFile(memory, "memory" + i, start, ONE_GB);
                    start += ONE_GB;
                }
            }

            final MutableDirectBuffer[] memoryBuffers = new MutableDirectBuffer[mappedMemoryBuffers.length];

            for (int i=0; i < memoryBuffers.length; i++)
            {
                memoryBuffers[i] = new UnsafeBuffer(mappedMemoryBuffers[i]);
            }

            return new MemoryLayout(metadataBuffer, memoryBuffers);
        }

        private static long sizeofBTree(
            long minimumBlockSize,
            long maximumBlockSize)
        {
            int orderCount = numberOfTrailingZeros(maximumBlockSize) - numberOfTrailingZeros(minimumBlockSize) + 1;
            return align(max(1L << orderCount << BITS_PER_BTREE_NODE_SHIFT >> BITS_PER_BYTE_SHIFT, Byte.BYTES), Byte.BYTES);
        }

        /**
         * Align a value to the next multiple up of alignment.
         * If the value equals an alignment multiple then it is returned unchanged.
         * <p>
         * This method executes without branching. This code is designed to be use in the fast path and should not
         * be used with negative numbers. Negative numbers will result in undefined behaviour.
         *
         * @param value     to be aligned up.
         * @param alignment to be used.
         * @return the value aligned to the next boundary.
         */
        private static long align(final long value, final long alignment)
        {
            return (value + (alignment - 1)) & ~(alignment - 1);
        }

        /**
         * Is a value a positive power of two.
         *
         * @param value to be checked.
         * @return true if the number is a positive power of two otherwise false.
         */
        private static boolean isPowerOfTwo(final long value)
        {
            return value > 0 && ((value & (~value + 1)) == value);
        }


    }
}
