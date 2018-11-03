/**
 * Copyright 2016-2018 The Reaktivity Project
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
import static org.reaktivity.nukleus.kafka.internal.test.TestUtil.asOctets;

import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.test.TestUtil;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;

public final class HeadersFWTest
{
    private HeaderFW.Builder headerRW = new HeaderFW.Builder();
    private final int offset = 11;
    int position = offset;
    private final MutableDirectBuffer rawHeaders = new UnsafeBuffer(allocateDirect(1000))
    {
        {
            position = headerRW.wrap(this, position, this.capacity())
                    .keyLen("header1".length())
                    .key(asOctets("header1"))
                    .valueLen("value1".length())
                    .value(asOctets("value1"))
                    .build().limit();
            position = headerRW.wrap(this, position, this.capacity())
                    .keyLen("header2".length())
                    .key(asOctets("header2"))
                    .valueLen("value1".length())
                    .value(asOctets("value1"))
                    .build().limit();
            position = headerRW.wrap(this, position, this.capacity())
                    .keyLen("header1".length())
                    .key(asOctets("header1"))
                    .valueLen("value2".length())
                    .value(asOctets("value2"))
                    .build().limit();
        }
    };
    private final HeadersFW headersRO = new HeadersFW();

    @Test
    public void shouldIteratOverSingleValuedHeader()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        Iterator<DirectBuffer> header = headersRO.headerSupplier().apply(TestUtil.asBuffer("header2"));
        assertTrue(header.hasNext());
        assertMatches("value1", header.next());
        assertFalse(header.hasNext());
    }

    @Test
    public void shouldIteratOverMultiValuedHeader()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        Iterator<DirectBuffer> header = headersRO.headerSupplier().apply(TestUtil.asBuffer("header1"));
        assertTrue(header.hasNext());
        assertMatches("value1", header.next());
        assertMatches("value2", header.next());
        assertFalse(header.hasNext());
    }

    @Test
    public void shouldMatchSingleHeaderConditionOnMultiplyOccuringHeader()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .item(b -> b.key("header1").value(asOctets("value1")))
                .build();
        assertTrue(headersRO.matches(headerConditions));
    }

    @Test
    public void shouldMatchSingleHeaderConditionOnSinglelyOccuringHeader()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(new UnsafeBuffer(new byte[100]), 0, 100)
                .item(b -> b.key("header2").value(asOctets("value1")))
                .build();
        assertTrue(headersRO.matches(headerConditions));
    }

    @Test
    public void shouldMatchEmptyHeaderConditions()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> emptyHeaderConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .build();
        assertTrue(headersRO.matches(emptyHeaderConditions));
    }

    @Test
    public void shouldMatchNullHeaderConditions()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        assertTrue(headersRO.matches(null));
    }

    @Test
    public void shouldMatchMultipleHeaderConditions()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .item(b -> b.key("header1").value(asOctets("value2")))
                .item(b -> b.key("header2").value(asOctets("value1")))
                .build();
        assertTrue(headersRO.matches(headerConditions));
    }

    @Test
    public void shouldNotMatchMultipleHeaderConditionsWithOneNotMatched()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .item(b -> b.key("header1").value(asOctets("value2")))
                .item(b -> b.key("header2").value(asOctets("junk")))
                .build();
        assertFalse(headersRO.matches(headerConditions));
    }

    @Test
    public void shouldNotMatchMultipleHeaderConditionsWithBothNotMatched()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .item(b -> b.key("header1").value(asOctets("junk")))
                .item(b -> b.key("header2").value(asOctets("junk")))
                .build();
        assertFalse(headersRO.matches(headerConditions));
    }

    @Test
    public void shouldMatchMultipleHeaderConditionsOnMultiplyOccuringHeader()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .item(b -> b.key("header1").value(asOctets("value1")))
                .item(b -> b.key("header1").value(asOctets("value2")))
                .build();
        assertTrue(headersRO.matches(headerConditions));
    }

    @Test
    public void shouldNotMatchMultipleHeaderConditionsOnMultiplyOccuringHeader()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .item(b -> b.key("header1").value(asOctets("value1")))
                .item(b -> b.key("header1").value(asOctets("junk")))
                .build();
        assertFalse(headersRO.matches(headerConditions));
    }

    @Test
    public void shouldNotMatchSingleHeaderConditionOnMultiplyOccuringHeader()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .item(b -> b.key("header1").value(asOctets("nope")))
                .build();
        assertFalse(headersRO.matches(headerConditions));
    }

    @Test
    public void shouldNotMatchSingleHeaderConditionOnSinglyOccuringHeader()
    {
        headersRO.wrap(rawHeaders,  offset, position);
        MutableDirectBuffer buffer = new UnsafeBuffer(new byte[100]);
        ListFW<KafkaHeaderFW> headerConditions =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(buffer, 0, buffer.capacity())
                .item(b -> b.key("header2").value(asOctets("nope")))
                .build();
        assertFalse(headersRO.matches(headerConditions));
    }

    private void assertMatches(
        String string,
        DirectBuffer buffer)
    {
        assertEquals(string, buffer.getStringWithoutLengthUtf8(0, buffer.capacity()));
    }
}
