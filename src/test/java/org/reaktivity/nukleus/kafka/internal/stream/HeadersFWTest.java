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
package org.reaktivity.nukleus.kafka.internal.stream;

import static java.nio.ByteBuffer.allocateDirect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.test.TestUtil;
import org.reaktivity.nukleus.kafka.internal.types.Varint32FW;

public final class HeadersFWTest
{
    private Varint32FW.Builder varint32RW = new Varint32FW.Builder();
    private final int offset = 13;
    int position = offset;
    private final MutableDirectBuffer buffer = new UnsafeBuffer(allocateDirect(1000))
    {
        {
            position = varint32RW.wrap(this, position, this.capacity()).set(7).build().limit();
            putStringWithoutLengthUtf8(position, "header1"); position += 7;
            position = varint32RW.wrap(this, position, this.capacity()).set(6).build().limit();
            putStringWithoutLengthUtf8(position, "value1"); position += 6;
            position = varint32RW.wrap(this, position, this.capacity()).set(7).build().limit();
            putStringWithoutLengthUtf8(position, "header2"); position += 7;
            position = varint32RW.wrap(this, position, this.capacity()).set(6).build().limit();
            putStringWithoutLengthUtf8(position, "value1"); position += 6;
            position = varint32RW.wrap(this, position, this.capacity()).set(7).build().limit();
            putStringWithoutLengthUtf8(position, "header1"); position += 7;
            position = varint32RW.wrap(this, position, this.capacity()).set(6).build().limit();
            putStringWithoutLengthUtf8(position, "value2"); position += 6;
        }
    };
    private final HeadersFW headersRO = new HeadersFW();

    @Test
    public void shouldIteratOverSingleValuedHeader()
    {
        headersRO.wrap(buffer,  offset, offset + position);
        Iterator<DirectBuffer> header = headersRO.headerSupplier().apply(TestUtil.asBuffer("header2"));
        assertTrue(header.hasNext());
        assertMatches("value1", header.next());
    }

    @Test
    public void shouldIteratOverMultiValuedHeader()
    {
        headersRO.wrap(buffer,  offset, offset + position);
        Iterator<DirectBuffer> header = headersRO.headerSupplier().apply(TestUtil.asBuffer("header1"));
        assertTrue(header.hasNext());
        assertMatches("value1", header.next());
        assertMatches("value2", header.next());
        assertFalse(header.hasNext());
    }

    private void assertMatches(
        String string,
        DirectBuffer buffer)
    {
        assertEquals(string, buffer.getStringWithoutLengthUtf8(0, buffer.capacity()));
    }
}
