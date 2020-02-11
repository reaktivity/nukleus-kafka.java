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

import static java.nio.file.StandardOpenOption.READ;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.cacheFile;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheTailSegment;

public abstract class KafkaCacheTailFile
{
    private final MappedByteBuffer readableByteBuf;

    protected final long baseOffset;
    protected final DirectBuffer readableBuf;
    protected final int readCapacity;

    protected KafkaCacheTailFile(
        KafkaCacheTailSegment segment,
        String extension)
    {
        final long baseOffset = segment.baseOffset();
        final Path tailFile = cacheFile(segment, extension);

        this.baseOffset = baseOffset;
        this.readableByteBuf = readInit(tailFile);
        this.readableBuf = new UnsafeBuffer(readableByteBuf);
        this.readCapacity = readableByteBuf.capacity();
    }

    private static MappedByteBuffer readInit(
        Path file)
    {
        MappedByteBuffer mapped = null;

        try (FileChannel channel = FileChannel.open(file, READ))
        {
            mapped = channel.map(MapMode.READ_ONLY, 0, channel.size());
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        assert mapped != null;
        return mapped;
    }
}
