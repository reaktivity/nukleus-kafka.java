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

import static org.reaktivity.nukleus.kafka.internal.util.BufferUtil.EMPTY_BYTE_ARRAY;

import java.util.Iterator;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;
import org.reaktivity.nukleus.kafka.internal.util.BufferUtil;

public final class HeadersFW
{
    private final DirectBuffer emptyBuffer = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    private final HeaderFW headerRO = new HeaderFW();

    private final HeaderValueIterator iterator = new HeaderValueIterator();

    private final DirectBuffer key = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final DirectBuffer value1 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final DirectBuffer value2 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    private final Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader = this::supplyHeader;

    private int offset;
    private int limit;
    private DirectBuffer buffer;

    public DirectBuffer buffer()
    {
        return buffer;
    }

    public int limit()
    {
        return limit;
    }

    public int offset()
    {
        return offset;
    }

    public int sizeof()
    {
        return limit - offset;
    }

    /**
     * Wraps a contiguous series of fetch message headers in Kafka fetch response format
     * (varint32 keyLength, octets key, varint32 valueLength, octects value)
     */
    public HeadersFW wrap(
        DirectBuffer buffer,
        int offset,
        int limit)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.limit = limit;
        return this;
    }

    public HeadersFW wrap(
        Flyweight headers)
    {
        return headers == null ?
                wrap(emptyBuffer, 0, 0) :
                wrap(headers.buffer(), headers.offset(), headers.limit());
    }

    public Function<DirectBuffer, Iterator<DirectBuffer>> headerSupplier()
    {
        return supplyHeader;
    }

    public boolean matches(
        ListFW<KafkaHeaderFW> headerConditions)
    {
        // Find first non-matching header condition. If not found, all headerConditions are fulfilled.
        return headerConditions == null || null != headerConditions.matchFirst(
            h ->
            {
                boolean[] matchFound = new boolean[]{false};
                headerSupplier().apply(BufferUtil.wrap(key,  h.key())).forEachRemaining(
                    v -> matchFound[0] = matchFound[0] ||
                             BufferUtil.wrap(value1, v).equals(BufferUtil.wrap(value2, h.value())));
                return matchFound[0];
            }
        );
    }

    @Override
    public String toString()
    {
        StringBuffer result = new StringBuffer();
        for (int offset = offset(); offset < limit; offset = headerRO.limit())
        {
            result.append(offset == offset() ? "" : ",");
            headerRO.wrap(buffer, offset, limit);
            String key = headerRO.key().get((b, o, l) ->
            {
                return b.getStringWithoutLengthUtf8(o,  l - o);
            });
            result.append(key);
            result.append('=');
            String value = headerRO.value().get((b, o, l) ->
            {
                return b.getStringWithoutLengthUtf8(o,  l - o);
            });
            result.append(value);
        }
        return result.toString();
    }

    private Iterator<DirectBuffer> supplyHeader(DirectBuffer headerKey)
    {
        return iterator.reset(headerKey);
    }

    private final class HeaderValueIterator implements Iterator<DirectBuffer>
    {
        private final DirectBuffer headerValue = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
        private final DirectBuffer compare1 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
        private final DirectBuffer compare2 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

        private DirectBuffer headerKey;
        private int position;

        @Override
        public boolean hasNext()
        {
            return position < limit();
        }

        @Override
        public DirectBuffer next()
        {
            headerRO.wrap(buffer, position, limit());
            OctetsFW value = headerRO.value();
            headerValue.wrap(value.buffer(), value.offset(), value.sizeof());
            position = headerRO.limit();
            advance();
            return headerValue;
        }

        private void advance()
        {
            while (position < limit())
            {
                headerRO.wrap(buffer, position, limit());
                if (matches(headerRO.key(), headerKey))
                {
                    break;
                }
                else
                {
                    position = headerRO.limit();
                }
            }
        }

        private Iterator<DirectBuffer> reset(
                DirectBuffer headerKey)
        {
            this.headerKey = headerKey;
            position = offset();
            advance();
            return this;
        }

        private boolean matches(OctetsFW octets, DirectBuffer buffer)
        {
            BufferUtil.wrap(compare1, octets);
            compare2.wrap(buffer);
            return compare1.equals(compare2);
        }
    }
}