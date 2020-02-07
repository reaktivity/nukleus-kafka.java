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

import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public abstract class KafkaCacheIndexFile extends KafkaCacheFile
{
    protected KafkaCacheIndexFile(
        MutableDirectBuffer writeBuffer,
        Path file,
        long baseOffset,
        int maxCapacity)
    {
        super(writeBuffer, file, baseOffset, maxCapacity);
    }

    protected KafkaCacheIndexFile(
        MutableDirectBuffer writeBuffer,
        Path writeFile,
        Path freezeFile,
        long baseOffset,
        int maxCapacity)
    {
        super(writeBuffer, writeFile, freezeFile, baseOffset, maxCapacity);
    }

    protected int value(
        int index)
    {
        return (int)(readableBuf.getLong(index << 3) & 0xFFFF_FFFF);
    }

    protected int seek(
        int key,
        int index)
    {
        final DirectBuffer buffer = readableBuf;
        final int maxIndex = (buffer.capacity() >> 3) - 1;

        // TODO: use index hint for linear probe before binary search

        int lowIndex = 0;
        int highIndex = maxIndex;

        while (lowIndex <= highIndex)
        {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final long entry = buffer.getLong(midIndex << 3);
            final int entryKey = (int)(entry >>> 32);

            if (entryKey < key)
            {
                lowIndex = midIndex + 1;
            }
            else if (entryKey > key)
            {
                highIndex = midIndex - 1;
            }
            else
            {
                return midIndex;
            }
        }

        return lowIndex <= highIndex && lowIndex >= 0 && highIndex <= maxIndex ? lowIndex : -1;
    }

    protected int scan(
        int key,
        int index)
    {
        final DirectBuffer buffer = readableBuf;
        final int maxIndex = (buffer.capacity() >> 3) - 1;

        for (int currentIndex = Math.max(index, 0); currentIndex <= maxIndex; currentIndex++)
        {
            final long entry = buffer.getLong(currentIndex << 3);
            final int entryKey = (int)(entry >>> 32);
            if (entryKey == key)
            {
                return currentIndex;
            }
        }

        return -1;
    }
}
