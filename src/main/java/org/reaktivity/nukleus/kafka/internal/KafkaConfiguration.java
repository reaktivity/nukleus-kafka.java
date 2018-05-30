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
package org.reaktivity.nukleus.kafka.internal;

import org.reaktivity.nukleus.Configuration;

public class KafkaConfiguration extends Configuration
{
    public static final String TOPIC_BOOTSTRAP_ENABLED = "nukleus.kafka.topic.bootstrap.enabled";

    public static final String FETCH_MAX_BYTES_PROPERTY = "nukleus.kafka.fetch.max.bytes";

    // "headers": cache messages during bootstrap only for topics with route header conditions
    // "all": cache messages during bootstrap for all topics
    public static final String MESSAGE_CACHE_PROACTIVE_PROPERTY = "nukleus.kafka.message.cache.proactive";

    // Maximum record batch size, corresponding to Kafka broker and topic configuration
    // property "max.message.bytes"
    public static final String FETCH_PARTITION_MAX_BYTES_PROPERTY = "nukleus.kafka.fetch.partition.max.bytes";

    private static final boolean TOPIC_BOOTSTRAP_ENABLED_DEFAULT = true;

    private static final int FETCH_MAX_BYTES_DEFAULT = 50 * 1024 * 1024;

    private static final int FETCH_PARTITION_MAX_BYTES_DEFAULT = 1 * 1024 * 1024;

    public static final String MESSAGE_CACHE_CAPACITY_PROPERTY = "nukleus.kafka.message.cache.capacity";

    public static final String MESSAGE_CACHE_BLOCK_CAPACITY_PROPERTY = "nukleus.kafka.message.cache.block.capacity";

    public static final int MESSAGE_CACHE_CAPACITY_DEFAULT = 128 * 1024 * 1024;

    public static final int MESSAGE_CACHE_BLOCK_CAPACITY_DEFAULT = 1024;

    public static final boolean DEFAULT_MESSAGE_CACHE_PROACTIVE = false;

    public KafkaConfiguration(
        Configuration config)
    {
        super(config);
    }

    public boolean topicBootstrapEnabled()
    {
        return getBoolean(TOPIC_BOOTSTRAP_ENABLED, TOPIC_BOOTSTRAP_ENABLED_DEFAULT);
    }

    public int fetchMaxBytes()
    {
        return getInteger(FETCH_MAX_BYTES_PROPERTY, FETCH_MAX_BYTES_DEFAULT);
    }

    public int fetchPartitionMaxBytes()
    {
        return getInteger(FETCH_PARTITION_MAX_BYTES_PROPERTY, FETCH_PARTITION_MAX_BYTES_DEFAULT);
    }

    public int messageCacheCapacity()
    {
        return getInteger(MESSAGE_CACHE_CAPACITY_PROPERTY, MESSAGE_CACHE_CAPACITY_DEFAULT);
    }

    public int messageCacheBlockCapacity()
    {
        return getInteger(MESSAGE_CACHE_BLOCK_CAPACITY_PROPERTY, MESSAGE_CACHE_BLOCK_CAPACITY_DEFAULT);
    }

    public boolean messageCacheBootstrapAll()
    {
        return getBoolean(MESSAGE_CACHE_PROACTIVE_PROPERTY, DEFAULT_MESSAGE_CACHE_PROACTIVE);
    }

}
