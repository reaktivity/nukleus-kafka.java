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
package org.reaktivity.nukleus.kafka.internal.util;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;

public final class BufferUtil
{
    private static final int STRING16_SIZE_LENGTH = BitUtil.SIZE_OF_SHORT;

    /*
     * Assigns a zero-based partition ID based on a hashCode using the same method as
     * https://apache.googlesource.com/kafka/+/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/
     * DefaultPartitioner.java
     */
    public static int partition(
        final int hashCode,
        final int partitionCount)
    {
        return (hashCode & 0x7fffffff) % partitionCount;
    }

    /*
     * Generates hash code for a fetchKey using the same method as
     * https://apache.googlesource.com/kafka/+/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/
     * DefaultPartitioner.java
     *
     * Adapted from the murmur2 method in
     * https://apache.googlesource.com/kafka/+/trunk/clients/src/main/java/org/apache/kafka/common/utils/Utils.java
     *
     * Generates 32 bit murmur2 hash from a range of bytes.
     */
    public static int defaultHashCode(
        final DirectBuffer buffer,
        final int offset,
        final int limit)
    {
        final int length = limit - offset;
        int seed = 0x9747b28c;
        // 'm' and 'r' are mixing constants generated offline.
        // They're not really 'magic', they just happen to work well.
        final int m = 0x5bd1e995;
        final int r = 24;
        // Initialize the hash to a random value
        int h = seed ^ length;
        int length4 = length / 4;
        for (int i = 0; i < length4; i++)
        {
            final int i4 = offset + i * 4;
            int k = (buffer.getByte(i4 + 0) & 0xff) + ((buffer.getByte(i4 + 1) & 0xff) << 8) +
                    ((buffer.getByte(i4 + 2) & 0xff) << 16) + ((buffer.getByte(i4 + 3) & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }
        // Handle the last few bytes of the input array
        final int remaining = length - 4 * length4;
        if (remaining == 3)
        {
            h ^= (buffer.getByte(offset + (length & ~3) + 2) & 0xff) << 16;
        }
        if (remaining >= 2)
        {
            h ^= (buffer.getByte(offset + (length & ~3) + 1) & 0xff) << 8;
        }
        if (remaining >= 1)
        {
            h ^= buffer.getByte(offset + (length & ~3)) & 0xff;
            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    public static boolean matches(
        final OctetsFW o1,
        final OctetsFW o2)
    {
        boolean result = o2 != null && o1 != null && o2.sizeof() == o1.sizeof();
        if (result)
        {
            final DirectBuffer buffer1 = o1.buffer();
            final DirectBuffer buffer2 = o2.buffer();
            final int offset1 = o1.offset();
            final int offset2 = o2.offset();
            for (int i=0; i < o1.sizeof(); i++)
            {
            if ((buffer1.getByte(offset1 + i) != buffer2.getByte(offset2 + i)))
                {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

    public static boolean matches(
        final OctetsFW octets,
        final String16FW string)
    {
        boolean result = string != null && octets != null && string.sizeof() - STRING16_SIZE_LENGTH == octets.sizeof();
        if (result)
        {
            final DirectBuffer octetsBuf = octets.buffer();
            final DirectBuffer stringBuf = string.buffer();
            final int octetsOffset = octets.offset();
            final int stringOffset = string.offset() + STRING16_SIZE_LENGTH;
            for (int i=0; i < octets.sizeof(); i++)
            {
                if (stringBuf.getByte(i + stringOffset) != octetsBuf.getByte(i + octetsOffset))
                {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

}
