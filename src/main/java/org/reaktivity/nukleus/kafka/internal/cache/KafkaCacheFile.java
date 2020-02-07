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
import static org.agrona.BufferUtil.address;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;

import org.agrona.DirectBuffer;
import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public abstract class KafkaCacheFile
{
    private final int maxCapacity;
    private final MutableDirectBuffer writeBuffer;
    private final ByteBuffer writeByteBuffer;
    private final FileChannel writer;

    private MappedByteBuffer readableByteBuf;
    private long readableAddress;

    protected final long baseOffset;
    protected final DirectBuffer readableBuf;
    protected final Path writeFile;
    protected final Path freezeFile;

    protected KafkaCacheFile(
        MutableDirectBuffer writeBuffer,
        Path file,
        long baseOffset,
        int maxCapacity)
    {
        this(writeBuffer, file, file, baseOffset, maxCapacity);
    }

    protected KafkaCacheFile(
        MutableDirectBuffer writeBuffer,
        Path writeFile,
        Path freezeFile,
        long baseOffset,
        int maxCapacity)
    {
        this.writeBuffer = requireNonNull(writeBuffer);
        this.writeByteBuffer = requireNonNull(writeBuffer.byteBuffer());
        this.readableByteBuf = readInit(writeFile, maxCapacity);
        this.readableBuf = new UnsafeBuffer(0, 0);
        this.writer = writeInit(writeFile);
        this.readableAddress = address(readableByteBuf);
        this.writeFile = writeFile;
        this.freezeFile = freezeFile;
        this.baseOffset = baseOffset;
        this.maxCapacity = maxCapacity;
    }

    int available()
    {
        return maxCapacity - readableBuf.capacity();
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

                final int newCapacity = readableBuf.capacity() + written;
                assert newCapacity <= maxCapacity;
                readableBuf.wrap(readableAddress, newCapacity);
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

            if (freezeFile != writeFile)
            {
                Files.delete(writeFile);

                IoUtil.unmap(readableByteBuf);
                this.readableByteBuf = readInit(freezeFile);
                this.readableAddress = address(readableByteBuf);
            }
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

    protected static Path filename(
        Path directory,
        long baseOffset,
        String extension)
    {
        return directory.resolve(String.format("%016x.%s", baseOffset, extension));
    }
}
