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
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaConditionFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaFilterFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public final class KafkaCursorFactory
{
    private final CRC32C checksum;
    private final KafkaCondition nullKeyInfo;

    public KafkaCursorFactory()
    {
        this.checksum = new CRC32C();
        this.nullKeyInfo = initNullKeyInfo(checksum);
    }

    public KafkaCursor newCursor(
        ArrayFW<KafkaFilterFW> filters)
    {
        KafkaCondition condition;

        if (filters.isEmpty())
        {
            condition = new KafkaCondition.None();
        }
        else
        {
            final List<KafkaCondition> asConditions = new ArrayList<>();
            filters.forEach(f -> asConditions.add(asCondition(f)));
            condition = new KafkaCondition.Or(asConditions);
        }

        return new KafkaCursor(condition);
    }

    private KafkaCondition asCondition(
        KafkaFilterFW filter)
    {
        final ArrayFW<KafkaConditionFW> conditions = filter.conditions();
        assert !conditions.isEmpty();
        List<KafkaCondition> asConditions = new ArrayList<>();
        conditions.forEach(c -> asConditions.add(asCondition(c)));
        return new KafkaCondition.And(asConditions);
    }

    private KafkaCondition asCondition(
        KafkaConditionFW condition)
    {
        KafkaCondition asCondition = null;

        switch (condition.kind())
        {
        case KafkaConditionFW.KIND_KEY:
            asCondition = asKeyCondition(condition.key());
            break;
        case KafkaConditionFW.KIND_HEADER:
            asCondition = asHeaderCondition(condition.header());
            break;
        }

        assert asCondition != null;
        return asCondition;
    }

    private KafkaCondition asKeyCondition(
        KafkaKeyFW key)
    {
        final OctetsFW value = key.value();

        return value == null ? nullKeyInfo : new KafkaCondition.Key(checksum, key);
    }

    private KafkaCondition asHeaderCondition(
        KafkaHeaderFW header)
    {
        return new KafkaCondition.Header(checksum, header);
    }

    private static KafkaCondition.Key initNullKeyInfo(
        CRC32C checksum)
    {
        final KafkaKeyFW nullKeyRO = new KafkaKeyFW.Builder()
                .wrap(new UnsafeBuffer(ByteBuffer.allocate(5)), 0, 5)
                .length(-1)
                .value((OctetsFW) null)
                .build();
        return new KafkaCondition.Key(checksum, nullKeyRO);
    }
}
