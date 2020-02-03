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
package org.reaktivity.nukleus.kafka.internal.util;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public final class BufferUtilTest
{

    private final MutableDirectBuffer buffer = new UnsafeBuffer(allocateDirect(100))
    {
        {
            // Make sure the code is not secretly relying upon memory being initialized to 0
            setMemory(0, capacity(), (byte) 0xab);
        }
    };

    @Test
    public void shouldGenerateHashForOneByteValue()
    {
        final int offset = 11;
        byte[] data = "a".getBytes(UTF_8);
        buffer.putBytes(offset, data);
        assertEquals(Utils.murmur2(data), BufferUtil.defaultHashCode(buffer, offset, offset + data.length));
    }

    @Test
    public void shouldGenerateHashForTwoByteValue()
    {
        final int offset = 45;
        byte[] data = "ab".getBytes(UTF_8);
        buffer.putBytes(offset, data);
        assertEquals(Utils.murmur2(data), BufferUtil.defaultHashCode(buffer, offset, offset + data.length));
    }

    @Test
    public void shouldGenerateHashForThreeByteValue()
    {
        final int offset = 67;
        byte[] data = "abc".getBytes(UTF_8);
        buffer.putBytes(offset, data);
        assertEquals(Utils.murmur2(data), BufferUtil.defaultHashCode(buffer, offset, offset + data.length));
    }

    @Test
    public void shouldGenerateHashForFourByteValue()
    {
        final int offset = 23;
        byte[] data = "abcd".getBytes(UTF_8);
        buffer.putBytes(offset, data);
        assertEquals(Utils.murmur2(data), BufferUtil.defaultHashCode(buffer, offset, offset + data.length));
    }

    @Test
    public void shouldGenerateHashFor40ByteValue()
    {
        final int offset = 38;
        byte[] data = "abcdefghij1234567890abcdefghij1234567890".getBytes(UTF_8);
        buffer.putBytes(offset, data);
        assertEquals(Utils.murmur2(data), BufferUtil.defaultHashCode(buffer, offset, offset + data.length));
    }

    @Test
    public void shouldGenerateHashFor41ByteValue()
    {
        final int offset = 38;
        byte[] data = "abcdefghij1234567890abcdefghij1234567890a".getBytes(UTF_8);
        buffer.putBytes(offset, data);
        assertEquals(Utils.murmur2(data), BufferUtil.defaultHashCode(buffer, offset, offset + data.length));
    }

    @Test
    public void shouldGenerateHashFor42ByteValue()
    {
        final int offset = 38;
        byte[] data = "abcdefghij1234567890abcdefghij1234567890ab".getBytes(UTF_8);
        buffer.putBytes(offset, data);
        assertEquals(Utils.murmur2(data), BufferUtil.defaultHashCode(buffer, offset, offset + data.length));
    }

    @Test
    public void shouldGenerateHashFor43ByteValue()
    {
        final int offset = 38;
        byte[] data = "abcdefghij1234567890abcdefghij1234567890abc".getBytes(UTF_8);
        buffer.putBytes(offset, data);
        assertEquals(Utils.murmur2(data), BufferUtil.defaultHashCode(buffer, offset, offset + data.length));
    }

    @Test
    public void shouldMatchSameValues()
    {
        OctetsFW value1 = octets("abcd");
        OctetsFW value2 = octets("abcd");
        assertTrue(BufferUtil.matches(value1, value2));
    }

    @Test
    public void shouldNotMatchWhenValue1IsLonger()
    {
        OctetsFW value1 = octets("abcde");
        OctetsFW value2 = octets("abcd");
        assertFalse(BufferUtil.matches(value1, value2));
    }

    @Test
    public void shouldNotMatchWhenValue2IsLonger()
    {
        OctetsFW value1 = octets("abcd");
        OctetsFW value2 = octets("abcde");
        assertFalse(BufferUtil.matches(value1, value2));
    }

    private OctetsFW octets(
        String value)
    {
        return new OctetsFW().wrap(new UnsafeBuffer(value.getBytes(US_ASCII)), 0, value.length());
    }

    @Test
    public void shouldNotMatchWhenValue1IsNull()
    {
        assertFalse(BufferUtil.matches(null, octets("abcd")));
    }

    @Test
    public void shouldNotMatchWhenValue2IsNull()
    {
        OctetsFW value1 = octets("abcd");
        OctetsFW value2 = null;
        assertFalse(BufferUtil.matches(value1, value2));
    }

    @Test
    public void shouldPartitionForNegativeHashCodes()
    {
        assertEquals(Utils.toPositive(-22220) % 5, BufferUtil.partition(-22220, 5));
        assertEquals(Utils.toPositive(-22221) % 5, BufferUtil.partition(-22221, 5));
        assertEquals(Utils.toPositive(-22222) % 5, BufferUtil.partition(-22222, 5));
        assertEquals(Utils.toPositive(-22223) % 5, BufferUtil.partition(-22223, 5));
        assertEquals(Utils.toPositive(-22224) % 5, BufferUtil.partition(-22224, 5));
    }

    @Test
    public void shouldPartitionForPositiveHashCodes()
    {
        assertEquals(0, BufferUtil.partition(22220, 4));
        assertEquals(1, BufferUtil.partition(22221, 4));
        assertEquals(2, BufferUtil.partition(22222, 4));
        assertEquals(3, BufferUtil.partition(22223, 4));
    }

}
