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

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSegment;

public abstract class KafkaCacheHeadFile
{
    private final MutableDirectBuffer writeBuffer;
    private final ByteBuffer writeByteBuffer;
    private final FileChannel writer;
    private final MappedByteBuffer readableByteBuf;

    protected final long baseOffset;
    protected final MutableDirectBuffer readableBuf;

    protected volatile int writeCapacity;
    protected volatile int readCapacity;

    protected KafkaCacheHeadFile(
        KafkaCacheSegment segment,
        String extension,
        MutableDirectBuffer writeBuffer,
        int writeCapacity)
    {
        final long baseOffset = segment.baseOffset();
        final Path headFile = segment.cacheFile(extension);

        this.baseOffset = baseOffset;
        this.readableByteBuf = readInit(headFile, writeCapacity);
        this.readableBuf = new UnsafeBuffer(readableByteBuf);
        this.writeBuffer = requireNonNull(writeBuffer);
        this.writeByteBuffer = requireNonNull(writeBuffer.byteBuffer());
        this.writer = writeInit(headFile);
        this.writeCapacity = writeCapacity;
    }

    int available()
    {
        return writeCapacity - readCapacity;
    }

    protected void deleteIfExists(
        KafkaCacheSegment segment,
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

    protected boolean write(
        DirectBuffer buffer,
        int index,
        int length)
    {
        final int available = available();
        final boolean writable = available >= length;

        if (writable)
        {
            try
            {
                writeByteBuffer.clear();
                writeBuffer.putBytes(0, buffer, index, length);
                writeByteBuffer.limit(length);

                final int written = writer.write(writeByteBuffer);
                assert written == length;

                readCapacity += written;
                assert readCapacity <= writeCapacity;
            }
            catch (IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        return writable;
    }

    void freeze()
    {
        try
        {
            writer.close();

            this.writeCapacity = readCapacity;
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private static MappedByteBuffer readInit(
        Path file,
        int capacity)
    {
        MappedByteBuffer mapped = null;

        try (FileChannel channel = FileChannel.open(file, CREATE, READ, WRITE))
        {
            mapped = channel.map(MapMode.READ_WRITE, 0, capacity);
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
        Path file)
    {
        FileChannel channel = null;

        try
        {
            channel = FileChannel.open(file, APPEND);
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }

        assert channel != null;
        return channel;
    }
}
