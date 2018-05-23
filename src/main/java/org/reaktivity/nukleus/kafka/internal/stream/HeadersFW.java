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

import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;

public final class HeadersFW
{
    private int offset;
    private int limit;
    private DirectBuffer buffer;

    private final HeaderFW headerRO = new HeaderFW();
    private final DirectBuffer header = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final DirectBuffer value1 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);
    private final DirectBuffer value2 = new UnsafeBuffer(EMPTY_BYTE_ARRAY);

    private final Function<DirectBuffer, DirectBuffer> supplyHeader = this::supplyHeader;

    public DirectBuffer buffer()
    {
        return buffer;
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

    public Function<DirectBuffer, DirectBuffer> headerSupplier()
    {
        return supplyHeader;
    }

    private DirectBuffer supplyHeader(DirectBuffer headerName)
    {
        DirectBuffer result = null;
        if (limit > offset)
        {
            for (int offset = this.offset; offset < limit; offset = headerRO.limit())
            {
                headerRO.wrap(buffer, offset, limit);
                if (matches(headerRO.key(), headerName))
                {
                    OctetsFW value = headerRO.value();
                    header.wrap(value.buffer(), value.offset(), value.sizeof());
                    result = header;
                    break;
                }
            }
        }
        return result;
    }

    private boolean matches(OctetsFW octets, DirectBuffer buffer)
    {
        value1.wrap(octets.buffer(), octets.offset(), octets.sizeof());
        value2.wrap(buffer);
        return value1.equals(value2);
    }
}