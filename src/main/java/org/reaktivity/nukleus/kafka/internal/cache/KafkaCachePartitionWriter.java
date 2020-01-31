/**
 * Copyright 2016-2019 The Reaktivity Project
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
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.agrona.LangUtil;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public final class KafkaCachePartitionWriter
{
    private final Path path;
    private final int segmentBytes;
    private final int partitionId;
    private final NavigableMap<Integer, KafkaCacheSegment> segmentsByOffset;

    KafkaCachePartitionWriter(
        KafkaConfiguration config,
        String clusterName,
        String topicName,
        int partitionId)
    {
        this.path = initDirectory(config.cacheDirectory(), clusterName, topicName, partitionId);
        this.segmentBytes = config.cacheSegmentBytes();
        this.partitionId = partitionId;
        this.segmentsByOffset = new ConcurrentSkipListMap<>();
    }

    public int id()
    {
        return partitionId;
    }

    public long progressOffset()
    {
        return -2; // EARLIEST, TODO
    }

    public void writeEntry(
        long offset,
        long timestamp,
        KafkaKeyFW key,
        ArrayFW<KafkaHeaderFW> headers,
        OctetsFW payload)
    {
        writeEntryStart(timestamp, key);
        writeEntryContinue(payload);
        writeEntryFinish(headers, offset);
    }

    public void writeEntryStart(
        long timestamp,
        KafkaKeyFW key)
    {
        // append timestamp and key to partition cache
    }

    public void writeEntryContinue(
        OctetsFW payload)
    {
        // append payload to partition cache
    }

    public void writeEntryFinish(
        ArrayFW<KafkaHeaderFW> headers,
        long offset)
    {
        // append headers to partition cache
    }

    static Path initDirectory(
        Path cacheDirectory,
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
