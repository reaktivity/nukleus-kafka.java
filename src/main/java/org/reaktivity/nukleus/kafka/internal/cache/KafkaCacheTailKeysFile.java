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

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.index;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.record;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.value;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.CACHE_EXTENSION_KEYS_INDEX;

import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheTailSegment;

public final class KafkaCacheTailKeysFile extends KafkaCacheTailIndexFile
{
    private final long baseOffset;
    final KafkaCacheTailKeysFile previousKeys;

    KafkaCacheTailKeysFile(
        KafkaCacheTailSegment segment,
        KafkaCacheTailKeysFile previousKeys)
    {
        super(segment, CACHE_EXTENSION_KEYS_INDEX);
        this.baseOffset = segment.baseOffset();
        this.previousKeys = previousKeys;
    }

    public long seekKey(
        int hash,
        long offset)
    {
        final long record = super.seekKey(hash);
        final int deltaOffset = (int)(baseOffset - offset);
        return super.scanKey(hash, record(index(record), deltaOffset));
    }

    public long scanKey(
        int hash,
        long record)
    {
        return super.scanKey(hash, record);
    }

    public long baseOffset(
        long record)
    {
        return baseOffset + value(record);
    }
}
