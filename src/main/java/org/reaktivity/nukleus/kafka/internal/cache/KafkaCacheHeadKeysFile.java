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

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.CACHE_EXTENSION_KEYS_INDEX;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.CACHE_EXTENSION_KEYS_SCAN;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.CACHE_EXTENSION_KEYS_SCAN_SORTING;

import java.nio.file.Path;

import org.agrona.MutableDirectBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheHeadSegment;

public final class KafkaCacheHeadKeysFile extends KafkaCacheHeadIndexFile
{
    private final KafkaCacheHeadSegment segment;
    final KafkaCacheTailKeysFile previousKeys;

    KafkaCacheHeadKeysFile(
        KafkaCacheHeadSegment segment,
        MutableDirectBuffer writeBuffer,
        int writeCapacity,
        KafkaCacheTailKeysFile previousKeys)
    {
        super(segment, CACHE_EXTENSION_KEYS_SCAN, writeBuffer, writeCapacity);
        this.segment = segment;
        this.previousKeys = previousKeys;
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

    public void toKeysIndex()
    {
        final Path keysScanFile = segment.cacheFile(CACHE_EXTENSION_KEYS_SCAN);
        final Path keysScanSortingFile = segment.cacheFile(CACHE_EXTENSION_KEYS_SCAN_SORTING);
        final Path keysIndexFile = segment.cacheFile(CACHE_EXTENSION_KEYS_INDEX);
        super.sortUnique(keysScanFile, keysScanSortingFile, keysIndexFile);
    }
}
