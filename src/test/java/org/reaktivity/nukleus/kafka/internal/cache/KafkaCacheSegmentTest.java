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
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursor;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorIndex;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorValue;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType.NONE;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
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

    private KafkaKeyFW.Builder keyRW;
    private ArrayFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW> headersRW;

    @Before
    public void setup()
    {
        this.keyRW = new KafkaKeyFW.Builder()
                .wrap(new UnsafeBuffer(ByteBuffer.allocate(1024)), 0, 1024);

        this.headersRW = new ArrayFW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                .wrap(new UnsafeBuffer(ByteBuffer.allocate(1024)), 0, 1024);
    }

    @Test
    public void shouldFreezeSegment()
    {
        KafkaConfiguration config = new KafkaConfiguration();
        Path directory = tempFolder.getRoot().toPath();

        KafkaCacheSegmentFactory factory = new KafkaCacheSegmentFactory(config);
        KafkaCacheSegment sentinel = factory.newSentinel(directory);
        KafkaCacheHeadSegment head = sentinel.nextSegment(0L);
        KafkaCacheTailSegment tail = head.freezeSegment(1L);

        assertEquals(head.nextSegment, tail.nextSegment);
        assertEquals(head.previousSegment, tail.previousSegment);
        assertEquals(head.previousSegment.nextSegment, tail);
    }

    @Test
    public void shouldSortHashScan()
    {
        KafkaConfiguration config = new KafkaConfiguration();
        Path directory = tempFolder.getRoot().toPath();

        KafkaCacheSegmentFactory factory = new KafkaCacheSegmentFactory(config);
        KafkaCacheSegment sentinel = factory.newSentinel(directory);
        KafkaCacheHeadSegment head = sentinel.nextSegment(0L);

        int cachedEntries = 1024;
        MutableDirectBuffer keyBytes = new UnsafeBuffer(new byte[Long.BYTES]);
        ArrayFW<KafkaHeaderFW> headers = headersRW.build();
        for (long offset = 0; offset < cachedEntries; offset++)
        {
            keyBytes.putLong(0, offset);
            KafkaKeyFW key = keyRW.rewrap()
                                  .length(keyBytes.capacity())
                                  .value(keyBytes, 0, keyBytes.capacity())
                                  .build();

            head.writeEntry(offset, currentTimeMillis(), key, headers, null, NONE);
        }

        KafkaCacheTailSegment tail = head.freezeSegment(cachedEntries);

        Int2IntHashMap hashScan = head.hashFile.toMap();
        Int2IntHashMap hashIndex = tail.hashFile.toMap();

        assertEquals(cachedEntries, hashScan.size());
        assertEquals(cachedEntries, hashIndex.size());
        assertEquals(hashScan, hashIndex);
    }

    @Test
    public void shouldSortKeysScan()
    {
        KafkaConfiguration config = new KafkaConfiguration();
        Path directory = tempFolder.getRoot().toPath();

        KafkaCacheSegmentFactory factory = new KafkaCacheSegmentFactory(config);
        KafkaCacheSegment sentinel = factory.newSentinel(directory);
        KafkaCacheHeadSegment head = sentinel.nextSegment(0L);

        int cachedEntries = 1024;
        MutableDirectBuffer keyBytes = new UnsafeBuffer(new byte[Long.BYTES]);
        ArrayFW<KafkaHeaderFW> headers = headersRW.build();
        for (long offset = 0; offset < cachedEntries; offset++)
        {
            keyBytes.putLong(0, offset >> 1);
            KafkaKeyFW key = keyRW.rewrap()
                                  .length(keyBytes.capacity())
                                  .value(keyBytes, 0, keyBytes.capacity())
                                  .build();

            head.writeEntry(offset, currentTimeMillis(), key, headers, null, NONE);
        }

        KafkaCacheTailSegment tail = head.freezeSegment(cachedEntries);

        Int2IntHashMap keysScan = head.keysFile.toMap();
        Int2IntHashMap keysIndex = tail.headSegment().keysFile.previousKeys.toMap();

        assertEquals(cachedEntries >> 1, keysScan.size());
        assertEquals(cachedEntries >> 1, keysIndex.size());
        assertEquals(keysScan, keysIndex);
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

        head.writeEntry(1, currentTimeMillis(), key, headers, value, NONE);

        CRC32C checksum = new CRC32C();
        checksum.update(key.buffer().byteArray(), 0, key.sizeof());
        int hash = (int) checksum.getValue();

        long headCursor = head.scanHash(hash, cursor(0, 0));

        final KafkaCacheSegment tail = head.freezeSegment(1L);

        long tailCursor = tail.scanHash(hash, cursor(0, 0));

        assertEquals(0, cursorIndex(headCursor));
        assertEquals(0, cursorValue(headCursor));
        assertEquals(headCursor, tailCursor);
    }
}
