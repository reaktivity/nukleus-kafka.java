/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.kafka.internal.config;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.bind.adapter.JsonbAdapter;

import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType;
import org.reaktivity.reaktor.config.With;
import org.reaktivity.reaktor.config.WithAdapterSpi;

public final class KafkaWithAdapter implements WithAdapterSpi, JsonbAdapter<With, JsonObject>
{
    private static final String DEFAULT_OFFSET_NAME = "defaultOffset";
    private static final String DELTA_TYPE_NAME = "deltaType";

    @Override
    public String type()
    {
        return KafkaNukleus.NAME;
    }

    @Override
    public JsonObject adaptToJson(
        With with)
    {
        KafkaWith kafkaWith = (KafkaWith) with;

        JsonObjectBuilder object = Json.createObjectBuilder();

        if (kafkaWith.defaultOffset != null)
        {
            object.add(DEFAULT_OFFSET_NAME, kafkaWith.defaultOffset.toString().toLowerCase());
        }

        if (kafkaWith.deltaType != null)
        {
            object.add(DELTA_TYPE_NAME, kafkaWith.deltaType.toString().toLowerCase());
        }

        return object.build();
    }

    @Override
    public With adaptFromJson(
        JsonObject object)
    {
        KafkaOffsetType defaultOffset = object.containsKey(DEFAULT_OFFSET_NAME)
                ? KafkaOffsetType.valueOf(object.getString(DEFAULT_OFFSET_NAME).toUpperCase())
                : null;

        KafkaDeltaType deltaType = object.containsKey(DELTA_TYPE_NAME)
                ? KafkaDeltaType.valueOf(object.getString(DELTA_TYPE_NAME).toUpperCase())
                : null;

        return new KafkaWith(defaultOffset, deltaType);
    }
}
