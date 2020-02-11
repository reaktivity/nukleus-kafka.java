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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.agrona.LangUtil;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSentinelSegment;

public final class KafkaCache
{
    public static final String TYPE_NAME = String.format("%s/cache", KafkaNukleus.NAME);

    private final Path cacheDirectory;
    private final KafkaCacheSegmentFactory cacheSegmentFactory;
    private final Map<String, Map<String, Map<Integer, KafkaCacheSentinelSegment>>> segmentsByClusterTopicPartition;

    public KafkaCache(
        KafkaConfiguration config)
    {
        this.cacheSegmentFactory = new KafkaCacheSegmentFactory(config);
        this.segmentsByClusterTopicPartition = new ConcurrentHashMap<>();
        this.cacheDirectory = config.cacheDirectory();
    }

    public KafkaCacheReader newReader()
    {
        return new KafkaCacheReader(this::supplySegment, cacheSegmentFactory.newCandidate());
    }

    public KafkaCacheWriter newWriter()
    {
        return new KafkaCacheWriter(this::supplySegment);
    }

    public KafkaCacheSentinelSegment supplySegment(
        String clusterName,
        String topicName,
        int partitionId)
    {
        final Map<String, Map<Integer, KafkaCacheSentinelSegment>> segmentsByTopicPartition =
                segmentsByClusterTopicPartition.computeIfAbsent(clusterName, c -> new ConcurrentHashMap<>());
        final Map<Integer, KafkaCacheSentinelSegment> sentinelsByPartition =
                segmentsByTopicPartition.computeIfAbsent(topicName, t -> new ConcurrentHashMap<>());
        return sentinelsByPartition.computeIfAbsent(partitionId,
            p -> cacheSegmentFactory.newSentinel(initDirectory(clusterName, topicName, partitionId)));
    }

    private Path initDirectory(
        String clusterName,
        String topicName,
        int partitionId)
    {
        final String partitionName = String.format("%s-%d", topicName, partitionId);
        final Path directory = cacheDirectory.resolve(clusterName).resolve(partitionName);

        try
        {
            Files.createDirectories(directory);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        return directory;
    }
}
