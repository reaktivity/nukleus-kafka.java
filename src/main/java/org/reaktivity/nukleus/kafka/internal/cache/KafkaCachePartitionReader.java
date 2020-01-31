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

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.END_OF_SEGMENT;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NavigableSet;
import java.util.TreeSet;

import org.agrona.LangUtil;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public final class KafkaCachePartitionReader
{
    private final Path directory;
    private final int segmentBytes;
    private final int partitionId;
    private final NavigableSet<KafkaCacheSegment> segments;

    private long progressOffset;
    private KafkaCacheSegment progressSegment;

    public KafkaCachePartitionReader(
        KafkaConfiguration config,
        String clusterName,
        String topicName,
        int partitionId)
    {
        this.directory = initDirectory(config.cacheDirectory(), clusterName, topicName, partitionId);
        this.segmentBytes = config.cacheSegmentBytes();
        this.partitionId = partitionId;
        this.segments = new TreeSet<>();
        this.progressOffset = -2L; // EARLIEST
    }

    public int id()
    {
        return partitionId;
    }

    public long progressOffset()
    {
        return progressOffset;
    }

    public KafkaCacheEntryFW readEntry(
        long nextOffset)
    {
        KafkaCacheEntryFW entry = null;

        int position = END_OF_SEGMENT;
        if (progressSegment != null)
        {
            position = progressSegment.position(nextOffset);
            if (position >= 0)
            {
                entry = progressSegment.entryAt(position);
            }
        }

        if (position == END_OF_SEGMENT)
        {
            KafkaCacheSegment nextSegment = nextSegment();

            if (nextSegment == null)
            {
                try
                {
                    Files.list(directory)
                         .filter(Files::isRegularFile)
                         .filter(Files::isReadable)
                         .map(Path::toString)
                         .filter(n -> n.endsWith(".log"))
                         .map(n -> n.substring(0, n.length() - ".log".length()))
                         .mapToInt(n -> Integer.parseInt(n, 16))
                         .filter(o -> o > progressOffset)
                         .forEach(this::initSegment);
                }
                catch (IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }

                nextSegment = nextSegment();
            }

            if (nextSegment != null)
            {
                progressSegment = nextSegment;
            }
        }

        if (entry != null)
        {
            progressOffset = entry.offset$() + 1;
        }

        return entry;
    }

    private KafkaCacheSegment nextSegment()
    {
        KafkaCacheSegment nextSegment = null;

        if (progressSegment != null)
        {
            nextSegment = segments.higher(progressSegment);
        }
        else if (!segments.isEmpty())
        {
            nextSegment = segments.first();
        }

        return nextSegment;
    }

    private KafkaCacheSegment initSegment(
        int baseOffset)
    {
        return new KafkaCacheSegment(directory, baseOffset, segmentBytes);
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
