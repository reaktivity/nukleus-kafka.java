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
package org.reaktivity.nukleus.kafka.internal.filter;

import java.nio.ByteBuffer;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;

public abstract class KafkaConditionInfo
{
    public final int hash;
    public final DirectBuffer value;

    public int segmentIndex;
    public long progressOffset;

    protected KafkaConditionInfo(
        CRC32C checksum,
        DirectBuffer buffer)
    {
        this(checksum, buffer, 0, buffer.capacity());
    }

    protected KafkaConditionInfo(
        CRC32C checksum,
        DirectBuffer buffer,
        int index,
        int length)
    {
        this.value = buffer;
        this.hash = computeHash(buffer, index, length, checksum);
    }

    public static final class Key extends KafkaConditionInfo
    {
        public Key(
            CRC32C checksum,
            KafkaKeyFW key)
        {
            super(checksum, key.buffer(), key.offset(), key.sizeof());
        }
    }

    public static final class Header extends KafkaConditionInfo
    {
        public Header(
            CRC32C checksum,
            KafkaHeaderFW header)
        {
            super(checksum, header.buffer(), header.offset(), header.sizeof());
        }
    }

    private static int computeHash(
        DirectBuffer buffer,
        int index,
        int length,
        CRC32C checksum)
    {
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        assert byteBuffer != null;
        byteBuffer.clear();
        byteBuffer.position(index);
        byteBuffer.limit(index + length);
        checksum.reset();
        checksum.update(byteBuffer);
        return (int) checksum.getValue();
    }
}
