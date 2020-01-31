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

import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public final class KafkaCacheSegment implements Comparable<KafkaCacheSegment>
{
    static final int END_OF_MESSAGES = -1;
    static final int END_OF_SEGMENT = -2;

    private final KafkaCacheEntryFW entryRO = new KafkaCacheEntryFW();

    private final int baseOffset;
    private final int capacity;
    private final KafkaCacheFile messages;
    private final KafkaCacheFile index;

    KafkaCacheSegment(
        Path directory,
        int baseOffset,
        int capacity)
    {
        this.baseOffset = baseOffset;
        this.capacity = capacity;
        this.messages = new KafkaCacheFile(directory, "log", baseOffset, capacity);
        this.index = new KafkaCacheFile(directory, "index", baseOffset, capacity);
    }

    public int position(
        long nextOffset)
    {
        final DirectBuffer buffer = index.readable();
        final int baseOffset = this.baseOffset;

        int lowIndex = 0;
        int highIndex = (buffer.capacity() >> 3) - 1;

        while (lowIndex <= highIndex)
        {
            final int midIndex = (lowIndex + highIndex) >>> 1;
            final long relativeEntry = buffer.getLong(midIndex << 3);
            final int relativeOffset = (int)(relativeEntry >>> 32);
            final int absoluteOffset = baseOffset + relativeOffset;

            if (absoluteOffset < nextOffset)
            {
                lowIndex = midIndex + 1;
            }
            else if (absoluteOffset > nextOffset)
            {
                highIndex = midIndex - 1;
            }
            else
            {
                return (int)(relativeEntry & 0xFFFF_FFFF);
            }
        }

        return lowIndex <= highIndex ? (int)(buffer.getLong(lowIndex << 3) & 0xFFFF_FFFF) : END_OF_MESSAGES;
    }

    public KafkaCacheEntryFW entryAt(
        int position)
    {
        return entryRO.tryWrap(messages.readable(), position, capacity);
    }

    @Override
    public int compareTo(
        KafkaCacheSegment that)
    {
        return this.baseOffset - that.baseOffset;
    }

    @Override
    public int hashCode()
    {
        return Integer.hashCode(baseOffset);
    }

    @Override
    public boolean equals(
        Object obj)
    {
        return this == obj ||
                (obj instanceof KafkaCacheSegment &&
                 equalTo((KafkaCacheSegment) obj));
    }

    private boolean equalTo(
        KafkaCacheSegment that)
    {
        return this.baseOffset == that.baseOffset;
    }
}
