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

import static java.lang.Integer.compareUnsigned;
import static java.lang.Integer.toUnsignedLong;

public final class KafkaCacheCursorRecord
{
    public static final long NEXT_SEGMENT = Long.MAX_VALUE - 1;
    public static final long RETRY_SEGMENT = Integer.MAX_VALUE - 1;

    public static int cursorIndex(
        long cursor)
    {
        return (int)(cursor >>> 32);
    }

    public static int cursorValue(
        long cursor)
    {
        return (int)(cursor & 0xFFFF_FFFFL);
    }

    public static long cursor(
        int index,
        int value)
    {
        return ((long) index << 32) | toUnsignedLong(value);
    }

    public static long nextIndex(
        long cursor)
    {
        return cursor(cursorIndex(cursor) + 1, cursorValue(cursor));
    }

    public static long previousIndex(
        long cursor)
    {
        return cursor(cursorIndex(cursor) - 1, cursorValue(cursor));
    }

    public static long nextValue(
        long cursor)
    {
        return cursor(cursorIndex(cursor), cursorValue(cursor) + 1);
    }

    public static long minByValue(
        long cursor1,
        long cursor2)
    {
        final int value1 = cursorValue(cursor1);
        final int value2 = cursorValue(cursor2);

        final int comparison = compareUnsigned(value1, value2);
        if (comparison < 0)
        {
            return cursor1;
        }
        else if (comparison > 0)
        {
            return cursor2;
        }
        else
        {
            return Long.min(cursor1, cursor2);
        }
    }

    public static long maxByValue(
        long cursor1,
        long cursor2)
    {
        final int value1 = cursorValue(cursor1);
        final int value2 = cursorValue(cursor2);

        if (value1 > value2)
        {
            return cursor1;
        }
        else if (value2 > value1)
        {
            return cursor2;
        }
        else
        {
            return Long.max(cursor1, cursor2);
        }
    }

    private KafkaCacheCursorRecord()
    {
        // no instances
    }
}
