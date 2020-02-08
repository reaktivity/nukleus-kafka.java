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

import static java.lang.Long.compareUnsigned;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.file.Files;
import java.nio.file.Path;

import org.agrona.IoUtil;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class KafkaCacheHashIndexFile extends KafkaCacheIndexFile
{
    KafkaCacheHashIndexFile(
        MutableDirectBuffer writeBuffer,
        Path directory,
        long baseOffset,
        int maxCapacity)
    {
        super(writeBuffer,
                filename(directory, baseOffset, "hscan"),
                //filename(directory, baseOffset, "hindex"),
                baseOffset, maxCapacity);
    }

    @Override
    void freeze()
    {
        try
        {
            if (freezeFile != writeFile)
            {
                final Path tempFreezeFile = freezeFile.resolveSibling(String.format("%s.tmp", freezeFile.getFileName()));

                Files.copy(writeFile, tempFreezeFile);

                try (FileChannel channel = FileChannel.open(tempFreezeFile, READ, WRITE))
                {
                    final ByteBuffer mapped = channel.map(MapMode.READ_WRITE, 0, channel.size());
                    final MutableDirectBuffer buffer = new UnsafeBuffer(mapped);

                    // TODO: better O(N) sort algorithm
                    int maxIndex = (buffer.capacity() >> 3) - 1;
                    for (int index = 0, priorIndex = index; index < maxIndex; priorIndex = ++index)
                    {
                        final long candidate = buffer.getLong((index + 1) << 3);
                        while (compareUnsigned(candidate, buffer.getLong(priorIndex << 3)) < 0)
                        {
                            buffer.putLong((priorIndex + 1) << 3, buffer.getLong(priorIndex << 3));
                            if (priorIndex-- == 0)
                            {
                                break;
                            }
                        }
                        buffer.putLong((priorIndex + 1) << 3, candidate);
                    }
                    IoUtil.unmap(mapped);
                }
                catch (IOException ex)
                {
                    LangUtil.rethrowUnchecked(ex);
                }

                Files.move(tempFreezeFile, freezeFile);

                super.freeze();
            }
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
