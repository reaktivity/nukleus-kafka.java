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

import static java.lang.System.currentTimeMillis;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition.OFFSET_LATEST;

import java.nio.file.Path;

import org.agrona.MutableDirectBuffer;

public final class KafkaCacheSegment extends KafkaCacheObject<KafkaCacheSegment>
{
    private final Path location;
    private final String name;
    private final int id;
    private final long baseOffset;
    private final long timestamp;

    private final KafkaCacheFile logFile;
    private final KafkaCacheFile deltaFile;
    private final KafkaCacheIndexFile indexFile;
    private final KafkaCacheIndexFile hashFile;
    private final KafkaCacheIndexFile keysFile;

    private long lastOffset;

    private int dirtyBytes;
    private long dirtySince;
    private long cleanableAt;

    public KafkaCacheSegment(
        KafkaCacheSegment segment,
        KafkaCacheTopicConfig config,
        MutableDirectBuffer appendBuf)
    {
        this(segment.location,
                config,
                segment.name,
                segment.id,
                segment.baseOffset,
                appendBuf);
    }

    public KafkaCacheSegment(
        Path location,
        KafkaCacheTopicConfig config,
        String name,
        int id,
        long baseOffset,
        MutableDirectBuffer appendBuf)
    {
        this.location = location;
        this.name = name;
        this.id = id;
        this.baseOffset = baseOffset;
        this.lastOffset = OFFSET_LATEST;
        this.timestamp = currentTimeMillis();
        this.logFile = new KafkaCacheFile.Log(location, baseOffset, config.segmentBytes, appendBuf);
        this.deltaFile = new KafkaCacheFile.Delta(location, baseOffset, config.segmentBytes, appendBuf);
        this.indexFile = new KafkaCacheFile.Index(location, baseOffset, config.segmentIndexBytes, appendBuf);
        this.hashFile = new KafkaCacheFile.HashScan(location, baseOffset, config.segmentIndexBytes, appendBuf);
        this.keysFile = new KafkaCacheFile.KeysScan(location, baseOffset, config.segmentIndexBytes, appendBuf);
    }

    public KafkaCacheSegment(
        Path location,
        String name,
        int id,
        long baseOffset,
        long lastOffset)
    {
        this.location = location;
        this.name = name;
        this.id = id;
        this.baseOffset = baseOffset;
        this.lastOffset = lastOffset;
        this.timestamp = currentTimeMillis();
        this.logFile = new KafkaCacheFile.Log(location, baseOffset);
        this.deltaFile = new KafkaCacheFile.Delta(location, baseOffset);
        this.indexFile = new KafkaCacheFile.Index(location, baseOffset);
        this.hashFile = new KafkaCacheFile.HashIndex(location, baseOffset);
        this.keysFile = new KafkaCacheFile.KeysIndex(location, baseOffset);
    }

    public Path location()
    {
        return location;
    }

    public String name()
    {
        return name;
    }

    public int id()
    {
        return id;
    }

    public long baseOffset()
    {
        return baseOffset;
    }

    public void lastOffset(
        long lastOffset)
    {
        assert lastOffset > this.lastOffset;
        this.lastOffset = lastOffset;
    }

    public long lastOffset()
    {
        return lastOffset;
    }

    public long nextOffset()
    {
        return lastOffset == OFFSET_LATEST ? baseOffset : lastOffset + 1;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public KafkaCacheFile logFile()
    {
        return logFile;
    }

    public KafkaCacheFile deltaFile()
    {
        return deltaFile;
    }

    public KafkaCacheIndexFile indexFile()
    {
        return indexFile;
    }

    public KafkaCacheIndexFile hashFile()
    {
        return hashFile;
    }

    public KafkaCacheIndexFile keysFile()
    {
        return keysFile;
    }

    public KafkaCacheSegment freeze()
    {
        logFile.freeze();
        deltaFile.freeze();
        indexFile.freeze();
        hashFile.freeze();
        keysFile.freeze();

        final KafkaCacheSegment frozen = new KafkaCacheSegment(location, name, id, baseOffset, lastOffset);

        frozen.dirtySince = dirtySince;
        frozen.dirtyBytes = dirtyBytes;
        frozen.cleanableAt = cleanableAt;

        return frozen;
    }

    public void delete()
    {
        logFile.delete();
        indexFile.delete();
        hashFile.delete();
        deltaFile.delete();
        keysFile.delete();
    }

    public long cleanableAt()
    {
        return cleanableAt;
    }

    public void cleanableAt(
        long cleanableAt)
    {
        this.cleanableAt = cleanableAt;
    }

    public long dirtySince()
    {
        return dirtySince;
    }

    public void markDirtyBytes(
        int dirtyBytes)
    {
        if (this.dirtyBytes == 0)
        {
            this.dirtySince = currentTimeMillis();
        }

        this.dirtyBytes += dirtyBytes;
    }

    public double cleanableDirtyRatio()
    {
        final int capacity = logFile.capacity();
        return capacity == 0 ? 0.0 : (double) dirtyBytes / capacity;
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s[%d] @ %d +%d", getClass().getSimpleName(), name, id, baseOffset, references());
    }

    @Override
    protected KafkaCacheSegment self()
    {
        return this;
    }

    @Override
    protected void onClosed()
    {
        logFile.close();
        indexFile.close();
        hashFile.close();
        deltaFile.close();
        keysFile.close();
    }
}
