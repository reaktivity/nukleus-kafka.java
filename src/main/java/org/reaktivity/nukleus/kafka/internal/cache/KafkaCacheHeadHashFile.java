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

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.CACHE_EXTENSION_HASH_INDEX;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.CACHE_EXTENSION_HASH_SCAN;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.CACHE_EXTENSION_HASH_SCAN_SORTING;

import java.nio.file.Path;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheHeadSegment;

public final class KafkaCacheHeadHashFile extends KafkaCacheHeadIndexFile
{
    private final KafkaCacheHeadSegment segment;

    KafkaCacheHeadHashFile(
        KafkaCacheHeadSegment segment,
        MutableDirectBuffer writeBuffer,
        int writeCapacity)
    {
        super(segment, CACHE_EXTENSION_HASH_SCAN, writeBuffer, writeCapacity);
        this.segment = segment;
    }

    public long seekHash(
        int hash,
        int position)
    {
        return super.seekValue(hash, position);
    }

    public long scanHash(
        int hash,
        long record)
    {
        return super.scanValue(hash, record);
    }

    public void toHashIndex()
    {
        final Path hashScanFile = segment.cacheFile(CACHE_EXTENSION_HASH_SCAN);
        final Path hashScanSortingFile = segment.cacheFile(CACHE_EXTENSION_HASH_SCAN_SORTING);
        final Path hashIndexFile = segment.cacheFile(CACHE_EXTENSION_HASH_INDEX);
        super.sort(hashScanFile, hashScanSortingFile, hashIndexFile);
    }
}
