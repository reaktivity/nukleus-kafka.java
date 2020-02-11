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

import static java.lang.System.currentTimeMillis;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheHeadSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheTailSegment;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public class KafkaCacheSegmentTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void shouldFreezeSegment()
    {
        KafkaConfiguration config = new KafkaConfiguration();
        Path directory = tempFolder.getRoot().toPath();

        KafkaCacheSegmentFactory factory = new KafkaCacheSegmentFactory(config);
        KafkaCacheSegment sentinel = factory.newSentinel(directory);
        KafkaCacheHeadSegment head = sentinel.nextSegment(0L);
        KafkaCacheTailSegment tail = head.freezeSegment();

        assertEquals(head.previousSegment, tail.previousSegment);
        assertEquals(head.nextSegment, tail.nextSegment);
        assertEquals(head.previousSegment.nextSegment, tail);
    }

    @Test
    public void shouldScanHash()
    {
        KafkaConfiguration config = new KafkaConfiguration();
        Path directory = tempFolder.getRoot().toPath();

        KafkaCacheSegmentFactory factory = new KafkaCacheSegmentFactory(config);
        KafkaCacheSegment sentinel = factory.newSentinel(directory);
        KafkaCacheHeadSegment head = sentinel.nextSegment(0L);

        DirectBuffer testBytes = new UnsafeBuffer("test".getBytes(UTF_8));
        KafkaKeyFW key = new KafkaKeyFW.Builder()
            .wrap(new UnsafeBuffer(ByteBuffer.allocate(1024)), 0, 1024)
            .length(4)
            .value(testBytes, 0, testBytes.capacity())
            .build();

        ArrayFW<KafkaHeaderFW> headers = new ArrayFW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
            .wrap(new UnsafeBuffer(ByteBuffer.allocate(1024)), 0, 1024)
            .item(h -> h.nameLen(testBytes.capacity()).name(testBytes, 0, testBytes.capacity())
                        .valueLen(testBytes.capacity()).value(testBytes, 0, testBytes.capacity()))
            .build();

        OctetsFW value = new OctetsFW.Builder()
            .wrap(new UnsafeBuffer(ByteBuffer.allocate(1024)), 0, 1024)
            .set(testBytes, 0, testBytes.capacity())
            .build();

        head.writeEntry(1, currentTimeMillis(), key, headers, value);

        CRC32C checksum = new CRC32C();
        checksum.update(key.buffer().byteArray(), 0, key.sizeof());
        int hash = (int) checksum.getValue();

        long headCursor = head.scanHash(hash, KafkaCacheCursorRecord.record(0, 0));

        final KafkaCacheSegment tail = head.freezeSegment();

        long tailCursor = tail.scanHash(hash, KafkaCacheCursorRecord.record(0, 0));

        assertEquals(0, KafkaCacheCursorRecord.index(headCursor));
        assertEquals(0, KafkaCacheCursorRecord.value(headCursor));
        assertEquals(headCursor, tailCursor);
    }
}
