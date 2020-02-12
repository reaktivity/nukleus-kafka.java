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

public final class KafkaCacheCursorRecord
{
    public static final long NEXT_SEGMENT = Long.MAX_VALUE - 1;
    public static final long RETRY_SEGMENT = Integer.MAX_VALUE - 1;

    public static int index(
        long record)
    {
        return (int)(record >>> 32);
    }

    public static int value(
        long record)
    {
        return (int)(record & 0xFFFF_FFFFL);
    }

    public static long record(
        int index,
        int value)
    {
        return ((long) index << 32) | value;
    }

    public static long nextIndex(
        long record)
    {
        return record(index(record) + 1, value(record));
    }

    public static long previousIndex(
        long record)
    {
        return record(index(record) - 1, value(record));
    }

    public static long nextValue(
        long record)
    {
        return record(index(record), value(record) + 1);
    }

    public static long minByValue(
        long record1,
        long record2)
    {
        final int value1 = value(record1);
        final int value2 = value(record2);

        final int comparison = compareUnsigned(value1, value2);
        if (comparison < 0)
        {
            return record1;
        }
        else if (comparison > 0)
        {
            return record2;
        }
        else
        {
            return Long.min(record1, record2);
        }
    }

    public static long maxByValue(
        long record1,
        long record2)
    {
        final int value1 = value(record1);
        final int value2 = value(record2);

        if (value1 > value2)
        {
            return record1;
        }
        else if (value2 > value1)
        {
            return record2;
        }
        else
        {
            return Long.max(record1, record2);
        }
    }

    private KafkaCacheCursorRecord()
    {
        // no instances
    }
}
