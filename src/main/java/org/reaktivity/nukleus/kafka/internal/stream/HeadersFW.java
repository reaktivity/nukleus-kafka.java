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
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;

public final class HeadersFW
{
    private final HeaderFW headerRO = new HeaderFW();

    private final HeaderValueIterator iterator = new HeaderValueIterator();

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

    public HeadersFW wrap(DirectBuffer buffer,
              int offset,
              int limit)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.limit = limit;
        return this;
    }

    public Function<DirectBuffer, Iterator<DirectBuffer>> headerSupplier()
    {
        return supplyHeader;
    }

    @Override
    public String toString()
    {
        StringBuffer result = new StringBuffer();
        for (int offset = offset(); offset < limit; offset = headerRO.limit())
        {
            result.append(offset == offset() ? "" : ",");
            headerRO.wrap(buffer, offset, limit);
            result.append(headerRO.key().get((b, o, l) ->
            {
                return b.getStringWithoutLengthUtf8(o,  l - o);
            }));
            result.append('=');
            result.append(headerRO.value().get((b, o, l) ->
            {
                return b.getStringWithoutLengthUtf8(o,  l - o);
            }));
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

        private DirectBuffer headerKey;
        private int position;
        private DirectBuffer nextResult;

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
            nextResult = null;
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
            value1.wrap(octets.buffer(), octets.offset(), octets.sizeof());
            value2.wrap(buffer);
            return value1.equals(value2);
        }
    }
}