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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition.Node;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public class KafkaCachePartitionTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void shouldCloseNodes() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());
        KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0);

        partition.close();

        assert partition.sentinel().closed();
    }

    @Test
    public void shouldSeekNotAfterNotFound() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            partition.append(10);
            partition.append(20);
            partition.append(30);

            assertEquals(partition.sentinel(), partition.seekNotAfter(5));
        }
    }

    @Test
    public void shouldSeekNotAfterEquals() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            partition.append(10);
            partition.append(20);
            partition.append(30);

            assertEquals(10, partition.seekNotAfter(10).segment().baseOffset());
        }
    }

    @Test
    public void shouldSeekNotAfterNotEquals() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            partition.append(10);
            partition.append(20);
            partition.append(30);

            assertEquals(10, partition.seekNotAfter(15).segment().baseOffset());
        }
    }

    @Test
    public void shouldSeekNotBeforeNotFound() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            partition.append(10);
            partition.append(20);
            partition.append(30);

            assertEquals(partition.sentinel(), partition.seekNotBefore(35));
        }
    }

    @Test
    public void shouldSeekNotBeforeEquals() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            partition.append(10);
            partition.append(20);
            partition.append(30);

            assertEquals(10, partition.seekNotBefore(10).segment().baseOffset());
        }
    }

    @Test
    public void shouldSeekNotBeforeNotEquals() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            partition.append(10);
            partition.append(20);
            partition.append(30);

            assertEquals(20, partition.seekNotBefore(15).segment().baseOffset());
        }
    }

    @Test
    public void shouldReplaceSegment() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            Node node10 = partition.append(10);
            Node node20 = partition.append(20);
            Node node30 = partition.append(30);

            Node sentinel = partition.sentinel();
            Node node10a = sentinel.next();
            Node node20a = node10a.next();
            Node node30a = node20a.next();

            assertNotSame(node10, node10a);
            assertSame(node10.replacement(), node10a);
            assertNotSame(node20, node20a);
            assertSame(node20.replacement(), node20a);
            assertSame(node30, node30a);
        }
    }

    @Test
    public void shouldRemoveSegment() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            partition.append(10);
            partition.append(20);
            partition.append(30);

            Node sentinel = partition.sentinel();
            Node node10 = sentinel.next();
            Node node20 = node10.next();
            Node node30 = node20.next();

            node20.remove();
            node20.close();

            assertSame(node10, node30.previous());
            assertSame(node30, node10.next());
        }
    }

    @Test
    public void shouldDescribeObject() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            assertEquals("test", partition.name());
            assertEquals(0, partition.id());
            assertEquals("[KafkaCachePartition] test[0] +1", partition.toString());
        }
    }

    public static class NodeTest
    {
        @Rule
        public TemporaryFolder tempFolder = new TemporaryFolder();

        @Test
        public void shouldCleanSegment() throws Exception
        {
            Path location = tempFolder.newFolder().toPath();
            KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

            MutableDirectBuffer writeBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

            KafkaKeyFW key = new KafkaKeyFW.Builder().wrap(writeBuffer, 0, writeBuffer.capacity())
                .length(4)
                .value(k -> k.set("test".getBytes(UTF_8)))
                .build();

            ArrayFW<KafkaHeaderFW> headers = new ArrayFW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                    .wrap(writeBuffer, key.limit(), writeBuffer.capacity())
                    .item(h -> h.nameLen(6).name(n -> n.set("header".getBytes(UTF_8)))
                                .valueLen(5).value(v -> v.set("value".getBytes(UTF_8))))
                    .build();

            OctetsFW value = new OctetsFW.Builder()
                    .wrap(writeBuffer, headers.limit(), 0)
                    .build();

            KafkaCacheEntryFW ancestorRO = new KafkaCacheEntryFW();

            try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0);
                    Node head10 = partition.append(10L))
            {
                partition.writeEntry(11L, 0L, key, headers, value, null, KafkaDeltaType.NONE);

                long keyHash = partition.computeKeyHash(key);
                KafkaCacheEntryFW ancestor = head10.findAndMarkAncestor(key, keyHash, ancestorRO);

                partition.writeEntry(12L, 0L, key, headers, value, ancestor, KafkaDeltaType.NONE);

                Node head15 = partition.append(15L);
                Node tail10 = head15.previous();

                long now = currentTimeMillis();
                tail10.segment().cleanableAt(now);
                tail10.clean(now);
                tail10.close();

                Node clean10 = tail10.replacement();

                assertNotNull(clean10);
                assertEquals("[Node] 10 +0", head10.toString());
                assertEquals("[Node] 10 +0", tail10.toString());
                assertEquals("[Node] 10 +1", clean10.toString());
            }
        }

        @Test
        public void shouldSeekAncestor() throws Exception
        {
            Path location = tempFolder.newFolder().toPath();
            KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

            MutableDirectBuffer writeBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

            KafkaKeyFW key = new KafkaKeyFW.Builder().wrap(writeBuffer, 0, writeBuffer.capacity())
                .length(4)
                .value(k -> k.set("test".getBytes(UTF_8)))
                .build();

            ArrayFW<KafkaHeaderFW> headers = new ArrayFW.Builder<>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
                    .wrap(writeBuffer, key.limit(), writeBuffer.capacity())
                    .build();

            OctetsFW value = new OctetsFW.Builder()
                    .wrap(writeBuffer, headers.limit(), 0)
                    .build();

            KafkaCacheEntryFW ancestorRO = new KafkaCacheEntryFW();

            try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0);
                    Node head10 = partition.append(10L))
            {
                partition.writeEntry(11L, 0L, key, headers, value, null, KafkaDeltaType.NONE);

                long keyHash = partition.computeKeyHash(key);
                KafkaCacheEntryFW ancestor = head10.findAndMarkAncestor(key, keyHash, ancestorRO);

                partition.writeEntry(12L, 0L, key, headers, value, ancestor, KafkaDeltaType.NONE);

                Node head15 = partition.append(15L);
                Node tail10 = head15.previous();

                Node seek10 = head15.seekAncestor(10L);

                assertEquals(seek10, tail10);
            }
        }

        @Test
        public void shouldDescribeObject() throws Exception
        {
            Path location = tempFolder.newFolder().toPath();
            KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

            try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0);
                    Node node10 = partition.append(10L))
            {
                assertEquals("[Node] 10 +1", node10.toString());
            }
        }

        @Test
        public void shouldDescribeSentinel() throws Exception
        {
            Path location = tempFolder.newFolder().toPath();
            KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

            try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
            {
                Node sentinel = partition.sentinel();
                assertEquals("[Node] sentinel +1", sentinel.toString());
            }
        }
    }
}
