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
package org.reaktivity.nukleus.kafka.internal;

import org.reaktivity.nukleus.Configuration;

public class KafkaConfiguration extends Configuration
{
    public static final boolean DEBUG = Boolean.getBoolean("nukleus.kafka.debug");

    public static final BooleanPropertyDef KAFKA_TOPIC_BOOTSTRAP_ENABLED;
    public static final IntPropertyDef KAFKA_FETCH_MAX_BYTES;
    public static final IntPropertyDef KAFKA_FETCH_PARTITION_MAX_BYTES;
    public static final IntPropertyDef KAFKA_MESSAGE_CACHE_CAPACITY;
    public static final IntPropertyDef KAFKA_MESSAGE_CACHE_BLOCK_CAPACITY;
    public static final BooleanPropertyDef KAFKA_MESSAGE_CACHE_PROACTIVE;
    public static final IntPropertyDef KAFKA_READ_IDLE_TIMEOUT;

    private static final ConfigurationDef KAFKA_CONFIG;

    static
    {
        final ConfigurationDef config = new ConfigurationDef("nukleus.kafka");
        KAFKA_TOPIC_BOOTSTRAP_ENABLED = config.property("topic.bootstrap.enabled", true);
        KAFKA_FETCH_MAX_BYTES = config.property("fetch.max.bytes", 50 * 1024 * 1024);
        // maximum record batch size, corresponding to Kafka broker and topic configuration property "max.message.bytes"
        KAFKA_FETCH_PARTITION_MAX_BYTES = config.property("fetch.partition.max.bytes", 1 * 1024 * 1024);
        KAFKA_MESSAGE_CACHE_CAPACITY = config.property("message.cache.capacity", 128 * 1024 * 1024);
        KAFKA_MESSAGE_CACHE_BLOCK_CAPACITY = config.property("message.cache.block.capacity", 1024);
        KAFKA_MESSAGE_CACHE_PROACTIVE = config.property("message.cache.proactive", false);
        KAFKA_READ_IDLE_TIMEOUT = config.property("read.idle.timeout", 5000);
        KAFKA_CONFIG = config;
    }

    public KafkaConfiguration(
        Configuration config)
    {
        super(KAFKA_CONFIG, config);
    }

    public boolean topicBootstrapEnabled()
    {
        return KAFKA_TOPIC_BOOTSTRAP_ENABLED.getAsBoolean(this);
    }

    public int fetchMaxBytes()
    {
        return KAFKA_FETCH_MAX_BYTES.getAsInt(this);
    }

    public int fetchPartitionMaxBytes()
    {
        return KAFKA_FETCH_PARTITION_MAX_BYTES.get(this);
    }

    public long messageCacheCapacity()
    {
        return KAFKA_MESSAGE_CACHE_CAPACITY.getAsInt(this);
    }

    public int messageCacheBlockCapacity()
    {
        return KAFKA_MESSAGE_CACHE_BLOCK_CAPACITY.getAsInt(this);
    }

    public boolean messageCacheProactive()
    {
        return KAFKA_MESSAGE_CACHE_PROACTIVE.getAsBoolean(this);
    }

    public int readIdleTimeout()
    {
        return KAFKA_READ_IDLE_TIMEOUT.getAsInt(this);
    }
}
