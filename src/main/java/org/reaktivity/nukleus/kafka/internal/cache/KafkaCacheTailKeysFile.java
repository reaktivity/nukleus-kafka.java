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

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.RETRY_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursor;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorIndex;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.CACHE_EXTENSION_KEYS_INDEX;

import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheTailSegment;

public final class KafkaCacheTailKeysFile extends KafkaCacheTailIndexFile
{
    private final long baseOffset;

    volatile KafkaCacheTailKeysFile previousKeys;
    volatile KafkaCacheTailKeysFile nextKeys;

    KafkaCacheTailKeysFile(
        KafkaCacheSegment segment,
        KafkaCacheTailKeysFile previousKeys)
    {
        super(segment, CACHE_EXTENSION_KEYS_INDEX);
        this.baseOffset = segment.baseOffset();
        this.previousKeys = previousKeys;
    }

    public long baseOffset(
        long cursor)
    {
        return baseOffset + cursorValue(cursor);
    }

    public long seekKey(
        int hash,
        long offset)
    {
        long cursor = super.seekKey(hash);
        if (cursor != RETRY_SEGMENT && cursor != NEXT_SEGMENT)
        {
            final int deltaOffset = (int)(offset - baseOffset);
            cursor = super.scanKey(hash, cursor(cursorIndex(cursor), deltaOffset));
        }
        return cursor;
    }

    public long scanKey(
        int hash,
        long cursor)
    {
        return super.scanKey(hash, cursor);
    }

    public long reverseSeekKey(
        int hash,
        long offset)
    {
        long cursor = super.reverseSeekKey(hash);
        if (cursor != RETRY_SEGMENT && cursor != NEXT_SEGMENT)
        {
            final int deltaOffset = (int)(offset - baseOffset);
            cursor = super.reverseScanKey(hash, cursor(cursorIndex(cursor), deltaOffset));
        }
        return cursor;
    }

    public void delete(
        KafkaCacheTailSegment segment)
    {
        super.deleteIfExists(segment, CACHE_EXTENSION_KEYS_INDEX);

        if (previousKeys != null)
        {
            previousKeys.nextKeys = nextKeys;
        }

        if (nextKeys != null)
        {
            nextKeys.previousKeys = previousKeys;
        }
    }

    public KafkaCacheTailKeysFile previousKeys()
    {
        return previousKeys;
    }

    public void replaces(
        KafkaCacheTailKeysFile dirtyKeys)
    {
        previousKeys = dirtyKeys.previousKeys;
        nextKeys = dirtyKeys.nextKeys;

        if (previousKeys != null)
        {
            previousKeys.nextKeys = this;
        }

        if (nextKeys != null)
        {
            nextKeys.previousKeys = this;
        }
    }

    @Override
    public String toString()
    {
        return String.format("%s %d", getClass().getSimpleName(), baseOffset);
    }
}
