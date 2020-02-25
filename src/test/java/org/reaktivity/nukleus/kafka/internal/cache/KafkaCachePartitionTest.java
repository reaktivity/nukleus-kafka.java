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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.nio.file.Path;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition.Node;

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
    public void shouldDescribeNameAndId() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            assertEquals("test", partition.name());
            assertEquals(0, partition.id());
        }
    }

    @Test
    public void shouldDescribeObject() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
        {
            assertEquals("[KafkaCachePartition] test[0] +1", partition.toString());
        }
    }

    public static class NodeTest
    {
        @Rule
        public TemporaryFolder tempFolder = new TemporaryFolder();

        @Test
        public void shouldDescribeObject() throws Exception
        {
            Path location = tempFolder.newFolder().toPath();
            KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

            try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0))
            {
                final Node sentinel = partition.sentinel();
                assertEquals("[Node] sentinel +1", sentinel.toString());
            }
        }
    }
}
