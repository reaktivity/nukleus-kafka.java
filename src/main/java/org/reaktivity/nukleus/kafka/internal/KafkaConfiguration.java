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
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCleanupPolicy;

public class KafkaConfiguration extends Configuration
{
    public static final boolean DEBUG = Boolean.getBoolean("nukleus.kafka.debug");

    public static final IntPropertyDef KAFKA_CLIENT_META_MAX_AGE;
    public static final IntPropertyDef KAFKA_CLIENT_DESCRIBE_MAX_AGE;
    public static final IntPropertyDef KAFKA_CLIENT_FETCH_MAX_WAIT_MILLIS;
    public static final IntPropertyDef KAFKA_CLIENT_FETCH_MAX_BYTES;
    public static final IntPropertyDef KAFKA_CLIENT_FETCH_PARTITION_MAX_BYTES;
    public static final PropertyDef<Path> KAFKA_CACHE_DIRECTORY;
    public static final PropertyDef<KafkaCacheCleanupPolicy> KAFKA_CACHE_CLEANUP_POLICY;
    public static final IntPropertyDef KAFKA_CACHE_MAX_MESSAGE_BYTES;
    public static final LongPropertyDef KAFKA_CACHE_RETENTION_MILLIS;
    public static final LongPropertyDef KAFKA_CACHE_RETENTION_BYTES;
    public static final LongPropertyDef KAFKA_CACHE_DELETE_RETENTION_MILLIS;
    public static final LongPropertyDef KAFKA_CACHE_MIN_COMPACTION_LAG_MILLIS;
    public static final LongPropertyDef KAFKA_CACHE_MAX_COMPACTION_LAG_MILLIS;
    public static final DoublePropertyDef KAFKA_CACHE_MIN_CLEANABLE_DIRTY_RATIO;
    public static final LongPropertyDef KAFKA_CACHE_SEGMENT_MILLIS;
    public static final IntPropertyDef KAFKA_CACHE_SEGMENT_BYTES;
    public static final IntPropertyDef KAFKA_CACHE_SEGMENT_INDEX_BYTES;
    public static final BooleanPropertyDef KAFKA_CACHE_SERVER_BOOTSTRAP;
    public static final BooleanPropertyDef KAFKA_CACHE_CLIENT_RECONNECT;
    public static final BooleanPropertyDef KAFKA_CACHE_SERVER_RECONNECT;

    private static final ConfigurationDef KAFKA_CONFIG;

    static
    {
        final ConfigurationDef config = new ConfigurationDef("nukleus.kafka");
        KAFKA_CLIENT_META_MAX_AGE = config.property("client.meta.max.age", 5 * 60);
        KAFKA_CLIENT_DESCRIBE_MAX_AGE = config.property("client.describe.max.age", 5 * 60);
        KAFKA_CLIENT_FETCH_MAX_WAIT_MILLIS = config.property("client.fetch.max.wait.millis", 500);
        KAFKA_CLIENT_FETCH_MAX_BYTES = config.property("client.fetch.max.bytes", 50 * 1024 * 1024);
        KAFKA_CLIENT_FETCH_PARTITION_MAX_BYTES = config.property("client.fetch.partition.max.bytes", 50 * 1024 * 1024);
        KAFKA_CACHE_DIRECTORY = config.property(Path.class, "cache.directory", (c, v) -> cacheDirectory(c, v), KafkaNukleus.NAME);
        KAFKA_CACHE_SERVER_BOOTSTRAP = config.property("cache.server.bootstrap", true);
        KAFKA_CACHE_SERVER_RECONNECT = config.property("cache.server.reconnect", true);
        KAFKA_CACHE_CLIENT_RECONNECT = config.property("cache.client.reconnect", false);
        KAFKA_CACHE_CLEANUP_POLICY = config.property(KafkaCacheCleanupPolicy.class, "cache.cleanup.policy",
                KafkaConfiguration::cleanupPolicy, "delete");
        KAFKA_CACHE_MAX_MESSAGE_BYTES = config.property("cache.max.message.bytes", 1000012);
        KAFKA_CACHE_RETENTION_MILLIS = config.property("cache.retention.ms", 604800000L);
        KAFKA_CACHE_RETENTION_BYTES = config.property("cache.retention.bytes", -1L);
        KAFKA_CACHE_DELETE_RETENTION_MILLIS = config.property("cache.delete.retention.ms", 86400000L);
        KAFKA_CACHE_MIN_COMPACTION_LAG_MILLIS = config.property("cache.min.compaction.lag.ms", 0L);
        KAFKA_CACHE_MAX_COMPACTION_LAG_MILLIS = config.property("cache.max.compaction.lag.ms", Long.MAX_VALUE);
        KAFKA_CACHE_MIN_CLEANABLE_DIRTY_RATIO = config.property("cache.min.cleanable.dirty.ratio", 0.5);
        KAFKA_CACHE_SEGMENT_MILLIS = config.property("cache.segment.ms", 604800000L);
        KAFKA_CACHE_SEGMENT_BYTES = config.property("cache.segment.bytes", 0x40000000);
        KAFKA_CACHE_SEGMENT_INDEX_BYTES = config.property("cache.segment.index.bytes", 0xA00000);
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

    public int clientMetaMaxAge()
    {
        return KAFKA_CLIENT_META_MAX_AGE.getAsInt(this);
    }

    public long clientDescribeMaxAge()
    {
        return KAFKA_CLIENT_DESCRIBE_MAX_AGE.getAsInt(this);
    }

    public int clientFetchMaxWaitMillis()
    {
        return KAFKA_CLIENT_FETCH_MAX_WAIT_MILLIS.getAsInt(this);
    }

    public int clientFetchMaxBytes()
    {
        return KAFKA_CLIENT_FETCH_MAX_BYTES.getAsInt(this);
    }

    public int clientFetchPartitionMaxBytes()
    {
        return KAFKA_CLIENT_FETCH_PARTITION_MAX_BYTES.get(this);
    }

    public Path cacheDirectory()
    {
        return KAFKA_CACHE_DIRECTORY.get(this);
    }

    public KafkaCacheCleanupPolicy cacheCleanupPolicy()
    {
        return KAFKA_CACHE_CLEANUP_POLICY.get(this);
    }

    public int cacheMaxMessageBytes()
    {
        return KAFKA_CACHE_MAX_MESSAGE_BYTES.get(this);
    }

    public long cacheRetentionBytes()
    {
        return KAFKA_CACHE_RETENTION_BYTES.getAsLong(this);
    }

    public long cacheRetentionMillis()
    {
        return KAFKA_CACHE_RETENTION_MILLIS.getAsLong(this);
    }

    public long cacheSegmentMillis()
    {
        return KAFKA_CACHE_SEGMENT_MILLIS.getAsLong(this);
    }

    public long cacheDeleteRetentionMillis()
    {
        return KAFKA_CACHE_DELETE_RETENTION_MILLIS.getAsLong(this);
    }

    public long cacheMinCompactionLagMillis()
    {
        return KAFKA_CACHE_MIN_COMPACTION_LAG_MILLIS.getAsLong(this);
    }

    public long cacheMaxCompactionLagMillis()
    {
        return KAFKA_CACHE_MAX_COMPACTION_LAG_MILLIS.getAsLong(this);
    }

    public double cacheMinCleanableDirtyRatio()
    {
        return KAFKA_CACHE_MIN_CLEANABLE_DIRTY_RATIO.getAsDouble(this);
    }

    public int cacheSegmentBytes()
    {
        return KAFKA_CACHE_SEGMENT_BYTES.getAsInt(this);
    }

    public int cacheSegmentIndexBytes()
    {
        return KAFKA_CACHE_SEGMENT_INDEX_BYTES.getAsInt(this);
    }

    public boolean cacheServerBootstrap()
    {
        return KAFKA_CACHE_SERVER_BOOTSTRAP.getAsBoolean(this);
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

    private static KafkaCacheCleanupPolicy cleanupPolicy(
        Configuration config,
        String cleanupPolicy)
    {
        return KafkaCacheCleanupPolicy.valueOf(cleanupPolicy.toUpperCase());
    }
}
