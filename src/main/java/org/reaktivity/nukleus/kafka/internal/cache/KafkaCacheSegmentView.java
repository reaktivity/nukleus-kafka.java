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

public final class KafkaCacheSegmentView extends KafkaCacheObjects.ReadOnly
{
    private final KafkaCacheSegment segment;

    public KafkaCacheSegmentView(
        KafkaCacheSegment segment)
    {
        super(segment);
        this.segment = segment;
    }

    public String name()
    {
        return segment.name();
    }

    public int index()
    {
        return segment.index();
    }

    public long baseOffset()
    {
        return segment.baseOffset();
    }

    public KafkaCacheFile logFile()
    {
        return segment.logFile();
    }

    public KafkaCacheFile deltaFile()
    {
        return segment.deltaFile();
    }

    public KafkaCacheIndexFile indexFile()
    {
        return segment.indexFile();
    }

    public KafkaCacheIndexFile hashFile()
    {
        return segment.hashFile();
    }

    public KafkaCacheIndexFile keysFile()
    {
        return segment.keysFile();
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s[%d] @ %d", getClass().getSimpleName(), name(), index(), baseOffset());
    }
}
