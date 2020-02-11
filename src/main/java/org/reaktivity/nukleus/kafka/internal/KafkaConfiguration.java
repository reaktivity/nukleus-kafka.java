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
package org.reaktivity.nukleus.kafka.internal;

import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_CACHE_DIRECTORY;

import java.nio.file.Path;

import org.reaktivity.nukleus.Configuration;

public class KafkaConfiguration extends Configuration
{
    public static final boolean DEBUG = Boolean.getBoolean("nukleus.kafka.debug");

    public static final IntPropertyDef KAFKA_META_MAX_AGE;
    public static final IntPropertyDef KAFKA_DESCRIBE_MAX_AGE;
    public static final IntPropertyDef KAFKA_FETCH_MAX_WAIT_MILLIS;
    public static final IntPropertyDef KAFKA_FETCH_MAX_BYTES;
    public static final IntPropertyDef KAFKA_FETCH_PARTITION_MAX_BYTES;
    public static final BooleanPropertyDef KAFKA_CACHE_SERVER_BOOTSTRAP;
    public static final PropertyDef<Path> KAFKA_CACHE_DIRECTORY;
    public static final IntPropertyDef KAFKA_CACHE_SEGMENT_LOG_BYTES;
    public static final IntPropertyDef KAFKA_CACHE_SEGMENT_INDEX_BYTES;
    public static final IntPropertyDef KAFKA_CACHE_SEGMENT_HASH_BYTES;
    public static final BooleanPropertyDef KAFKA_CACHE_CLIENT_RECONNECT;
    public static final BooleanPropertyDef KAFKA_CACHE_SERVER_RECONNECT;

    private static final ConfigurationDef KAFKA_CONFIG;

    static
    {
        final ConfigurationDef config = new ConfigurationDef("nukleus.kafka");
        KAFKA_META_MAX_AGE = config.property("meta.max.age", 5 * 60);
        KAFKA_DESCRIBE_MAX_AGE = config.property("describe.max.age", 5 * 60);
        KAFKA_FETCH_MAX_WAIT_MILLIS = config.property("fetch.max.wait.millis", 500);
        KAFKA_FETCH_MAX_BYTES = config.property("fetch.max.bytes", 50 * 1024 * 1024);
        KAFKA_FETCH_PARTITION_MAX_BYTES = config.property("fetch.partition.max.bytes", 50 * 1024 * 1024);
        KAFKA_CACHE_DIRECTORY = config.property(Path.class, "cache.directory", (c, v) -> cacheDirectory(c, v), KafkaNukleus.NAME);
        KAFKA_CACHE_SEGMENT_LOG_BYTES = config.property("cache.segment.log.bytes", 1 * 1024 * 1024);
        KAFKA_CACHE_SEGMENT_INDEX_BYTES = config.property("cache.segment.index.bytes", 256 * 1024);
        KAFKA_CACHE_SEGMENT_HASH_BYTES = config.property("cache.segment.hash.bytes", 16 * 1024);
        KAFKA_CACHE_SERVER_BOOTSTRAP = config.property("cache.server.bootstrap", true);
        KAFKA_CACHE_SERVER_RECONNECT = config.property("cache.server.reconnect", true);
        KAFKA_CACHE_CLIENT_RECONNECT = config.property("cache.client.reconnect", false);
        KAFKA_CONFIG = config;
    }

    public KafkaConfiguration()
    {
        this(new Configuration());
    }

    public KafkaConfiguration(
        Configuration config)
    {
        super(KAFKA_CONFIG, config);
    }

    public boolean cacheServerBootstrap()
    {
        return KAFKA_CACHE_SERVER_BOOTSTRAP.getAsBoolean(this);
    }

    public int metaMaxAge()
    {
        return KAFKA_META_MAX_AGE.getAsInt(this);
    }

    public long describeMaxAge()
    {
        return KAFKA_DESCRIBE_MAX_AGE.getAsInt(this);
    }

    public int fetchMaxWaitMillis()
    {
        return KAFKA_FETCH_MAX_WAIT_MILLIS.getAsInt(this);
    }

    public int fetchMaxBytes()
    {
        return KAFKA_FETCH_MAX_BYTES.getAsInt(this);
    }

    public int fetchPartitionMaxBytes()
    {
        return KAFKA_FETCH_PARTITION_MAX_BYTES.get(this);
    }

    public Path cacheDirectory()
    {
        return KAFKA_CACHE_DIRECTORY.get(this);
    }

    public int cacheSegmentLogBytes()
    {
        return KAFKA_CACHE_SEGMENT_LOG_BYTES.getAsInt(this);
    }

    public int cacheSegmentIndexBytes()
    {
        return KAFKA_CACHE_SEGMENT_INDEX_BYTES.getAsInt(this);
    }

    public int cacheSegmentHashBytes()
    {
        return KAFKA_CACHE_SEGMENT_HASH_BYTES.getAsInt(this);
    }

    public boolean cacheClientReconnect()
    {
        return KAFKA_CACHE_CLIENT_RECONNECT.getAsBoolean(this);
    }

    public boolean cacheServerReconnect()
    {
        return KAFKA_CACHE_SERVER_RECONNECT.getAsBoolean(this);
    }

    private static Path cacheDirectory(
        Configuration config,
        String cacheDirectory)
    {
        return REAKTOR_CACHE_DIRECTORY.get(config).resolve(cacheDirectory);
    }
}
