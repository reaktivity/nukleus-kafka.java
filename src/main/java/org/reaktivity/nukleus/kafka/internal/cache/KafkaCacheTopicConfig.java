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

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import org.agrona.DirectBuffer;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;

public final class KafkaCacheTopicConfig
{
    private static final String16FW CLEANUP_POLICY = new String16FW("cleanup.policy");
    private static final String16FW MAX_MESSAGE_BYTES = new String16FW("max.message.bytes");
    private static final String16FW SEGMENT_BYTES = new String16FW("segment.bytes");
    private static final String16FW SEGMENT_INDEX_BYTES = new String16FW("segment.index.bytes");
    private static final String16FW SEGMENT_MILLIS = new String16FW("segment.ms");
    private static final String16FW RETENTION_BYTES = new String16FW("retention.bytes");
    private static final String16FW RETENTION_MILLIS = new String16FW("retention.ms");

    private static final String16FW CLEANUP_POLICY_COMPACT = new String16FW("compact");
    private static final String16FW CLEANUP_POLICY_DELETE = new String16FW("delete");
    private static final String16FW CLEANUP_POLICY_COMPACT_DELETE = new String16FW("compact, delete");

    public volatile KafkaCacheCleanupPolicy cleanupPolicy;
    public volatile int maxMessageBytes;
    public volatile int segmentBytes;
    public volatile int segmentIndexBytes;
    public volatile long segmentMillis;
    public volatile long retentionBytes;
    public volatile long retentionMillis;

    private static final Map<String16FW, BiConsumer<KafkaCacheTopicConfig, String16FW>> CHANGE_HANDLERS;

    static
    {
        final HashMap<String16FW, BiConsumer<KafkaCacheTopicConfig, String16FW>> changeHandlers = new HashMap<>();
        changeHandlers.put(CLEANUP_POLICY, KafkaCacheTopicConfig::onCleanupPolicyChanged);
        changeHandlers.put(MAX_MESSAGE_BYTES, KafkaCacheTopicConfig::onMaxMessageBytesChanged);
        changeHandlers.put(SEGMENT_BYTES, KafkaCacheTopicConfig::onSegmentBytesChanged);
        changeHandlers.put(SEGMENT_INDEX_BYTES, KafkaCacheTopicConfig::onSegmentIndexBytesChanged);
        changeHandlers.put(SEGMENT_MILLIS, KafkaCacheTopicConfig::onSegmentMillisChanged);
        changeHandlers.put(RETENTION_BYTES, KafkaCacheTopicConfig::onRetentionBytesChanged);
        changeHandlers.put(RETENTION_MILLIS, KafkaCacheTopicConfig::onRetentionMillisChanged);
        CHANGE_HANDLERS = changeHandlers;
    }

    public KafkaCacheTopicConfig(
        KafkaConfiguration config)
    {
        this.cleanupPolicy = config.cacheCleanupPolicy();
        this.maxMessageBytes = config.cacheMaxMessageBytes();
        this.segmentBytes = config.cacheSegmentBytes();
        this.segmentIndexBytes = config.cacheSegmentIndexBytes();
        this.segmentMillis = config.cacheSegmentMillis();
        this.retentionBytes = config.cacheRetentionBytes();
        this.retentionMillis = config.cacheRetentionMillis();

    }

    public void onTopicConfigChanged(
        String16FW configName,
        String16FW configValue)
    {
        final BiConsumer<KafkaCacheTopicConfig, String16FW> handler = CHANGE_HANDLERS.get(configName);
        if (handler != null)
        {
            handler.accept(this, configValue);
        }
    }

    private static void onCleanupPolicyChanged(
        KafkaCacheTopicConfig topicConfig,
        String16FW value)
    {
        if (CLEANUP_POLICY_COMPACT.equals(value))
        {
            topicConfig.cleanupPolicy = KafkaCacheCleanupPolicy.COMPACT;
        }
        else if (CLEANUP_POLICY_DELETE.equals(value))
        {
            topicConfig.cleanupPolicy = KafkaCacheCleanupPolicy.DELETE;
        }
        else if (CLEANUP_POLICY_COMPACT_DELETE.equals(value))
        {
            topicConfig.cleanupPolicy = KafkaCacheCleanupPolicy.COMPACT_AND_DELETE;
        }
        else
        {
            System.out.format("Unrecognized cleanup policy: %s\n", value.asString());
            topicConfig.cleanupPolicy = null;
        }
    }

    private static void onMaxMessageBytesChanged(
        KafkaCacheTopicConfig topicConfig,
        String16FW value)
    {
        final DirectBuffer text = value.value();
        topicConfig.maxMessageBytes = text.parseIntAscii(0, text.capacity());
    }

    private static void onSegmentBytesChanged(
        KafkaCacheTopicConfig topicConfig,
        String16FW value)
    {
        final DirectBuffer text = value.value();
        topicConfig.segmentBytes = text.parseIntAscii(0, text.capacity());
    }

    private static void onSegmentIndexBytesChanged(
        KafkaCacheTopicConfig topicConfig,
        String16FW value)
    {
        final DirectBuffer text = value.value();
        topicConfig.segmentIndexBytes = text.parseIntAscii(0, text.capacity());
    }

    private static void onSegmentMillisChanged(
        KafkaCacheTopicConfig topicConfig,
        String16FW value)
    {
        final DirectBuffer text = value.value();
        topicConfig.segmentMillis = text.parseLongAscii(0, text.capacity());
    }

    private static void onRetentionBytesChanged(
        KafkaCacheTopicConfig topicConfig,
        String16FW value)
    {
        final DirectBuffer text = value.value();
        topicConfig.retentionBytes = text.parseLongAscii(0, text.capacity());
    }

    private static void onRetentionMillisChanged(
        KafkaCacheTopicConfig topicConfig,
        String16FW value)
    {
        final DirectBuffer text = value.value();
        topicConfig.retentionMillis = text.parseLongAscii(0, text.capacity());
    }
}
