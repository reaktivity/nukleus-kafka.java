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
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;

import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheTailSegment;

public abstract class KafkaCacheTailFile
{
    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    protected final MutableDirectBuffer readableBuf;
    protected final int readCapacity;

    protected KafkaCacheTailFile(
        KafkaCacheSegment segment,
        String extension)
    {
        final Path tailFile = segment.cacheFile(extension);
        final ByteBuffer readableByteBuf = readInit(tailFile);

        this.readableBuf = new UnsafeBuffer(readableByteBuf);
        this.readCapacity = readableByteBuf.capacity();
    }

    protected final void deleteIfExists(
        KafkaCacheTailSegment segment,
        String extension)
    {
        try
        {
            Files.deleteIfExists(segment.cacheFile(extension));
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static ByteBuffer readInit(
        Path file)
    {
        ByteBuffer mapped = null;

        try (FileChannel channel = FileChannel.open(file, READ, WRITE))
        {
            if (channel.size() == 0)
            {
                mapped = EMPTY_BUFFER;
            }
            else
            {
                mapped = channel.map(MapMode.READ_WRITE, 0, channel.size());
            }
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        assert mapped != null;
        return mapped;
    }
}
