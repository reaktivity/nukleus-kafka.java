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
import java.util.Collections;
import java.util.List;
import java.util.zip.CRC32C;

import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaConditionFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaFilterFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public final class KafkaFilterInfoFactory
{
    private static final List<KafkaFilterInfo> EMPTY_FILTER_INFOS = Collections.emptyList();

    private final KafkaConditionInfo nullKeyInfo;

    private final CRC32C checksum;

    public KafkaFilterInfoFactory()
    {
        this.checksum = new CRC32C();
        this.nullKeyInfo = initNullKeyInfo(checksum);
    }

    public List<KafkaFilterInfo> asFilterInfos(
        ArrayFW<KafkaFilterFW> filters)
    {
        List<KafkaFilterInfo> filterInfos;
        if (filters.isEmpty())
        {
            filterInfos = EMPTY_FILTER_INFOS;
        }
        else
        {
            final List<KafkaFilterInfo> newFilterInfos = new ArrayList<>();
            filters.forEach(f -> newFilterInfos.add(asFilterInfo(f)));
            filterInfos = newFilterInfos;
        }
        return filterInfos;
    }

    private KafkaFilterInfo asFilterInfo(
        KafkaFilterFW filter)
    {
        List<KafkaConditionInfo> conditionInfos = new ArrayList<>();
        filter.conditions().forEach(c -> conditionInfos.add(asConditionInfo(c)));
        return new KafkaFilterInfo(conditionInfos);
    }

    private KafkaConditionInfo asConditionInfo(
        KafkaConditionFW condition)
    {
        KafkaConditionInfo conditionInfo = null;

        switch (condition.kind())
        {
        case KafkaConditionFW.KIND_KEY:
            conditionInfo = asKeyConditionInfo(condition.key());
            break;
        case KafkaConditionFW.KIND_HEADER:
            conditionInfo = asHeaderConditionInfo(condition.header());
            break;
        }

        assert conditionInfo != null;
        return conditionInfo;
    }

    private KafkaConditionInfo asKeyConditionInfo(
        KafkaKeyFW key)
    {
        final OctetsFW value = key.value();

        return value == null ? nullKeyInfo : new KafkaConditionInfo.Key(checksum, key);
    }

    private KafkaConditionInfo asHeaderConditionInfo(
        KafkaHeaderFW header)
    {
        return new KafkaConditionInfo.Header(checksum, header);
    }

    private static KafkaConditionInfo.Key initNullKeyInfo(
        CRC32C checksum)
    {
        final KafkaKeyFW nullKeyRO = new KafkaKeyFW.Builder()
                .wrap(new UnsafeBuffer(ByteBuffer.allocate(5)), 0, 5)
                .length(-1)
                .value((OctetsFW) null)
                .build();
        return new KafkaConditionInfo.Key(checksum, nullKeyRO);
    }
}
