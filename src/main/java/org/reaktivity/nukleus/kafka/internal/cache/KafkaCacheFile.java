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

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;

public final class KafkaCacheFile
{
    private final MappedByteBuffer readableByteBuf;
    private final FileChannel writable;
    private final DirectBuffer readableBuf;

    KafkaCacheFile(
        Path directory,
        String extension,
        int offset,
        int capacity)
    {
        final Path file = directory.resolve(String.format("%08x.%s", offset, extension));
        this.readableByteBuf = readInit(file, capacity);
        this.readableBuf = new UnsafeBuffer(readableByteBuf);
        this.writable = writeInit(file);
    }

    DirectBuffer readable()
    {
        return readableBuf;
    }

    WritableByteChannel writable()
    {
        return writable;
    }

    private static MappedByteBuffer readInit(
        Path path,
        int capacity)
    {
        MappedByteBuffer mapped = null;

        try (FileChannel channel = FileChannel.open(path, CREATE, READ, WRITE))
        {
            mapped = channel.map(MapMode.READ_ONLY, 0, capacity);
            channel.truncate(0L);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        assert mapped != null;
        return mapped;
    }

    private static FileChannel writeInit(
        Path path)
    {
        FileChannel channel = null;

        try
        {
            channel = FileChannel.open(path, APPEND);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        assert channel != null;
        return channel;
    }
}
