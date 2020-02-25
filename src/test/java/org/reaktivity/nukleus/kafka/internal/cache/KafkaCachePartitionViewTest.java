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

import java.nio.file.Path;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartitionView.NodeView;

public class KafkaCachePartitionViewTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void shouldCloseNodes() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());
        KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0);
        KafkaCachePartitionView partitionView = partition.acquire(KafkaCachePartitionView::new);

        partition.append(10L);
        partitionView.sentinel().next();

        partition.close();
        partitionView.close();

        assert partitionView.sentinel().closed();
        assert partition.sentinel().closed();
    }

    @Test
    public void shouldDescribeObject() throws Exception
    {
        Path location = tempFolder.newFolder().toPath();
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

        try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0);
                KafkaCachePartitionView partitionView = partition.acquire(KafkaCachePartitionView::new))
        {
            assertEquals("test", partitionView.name());
            assertEquals(0, partitionView.id());
            assertEquals("[KafkaCachePartitionView] test[0]", partitionView.toString());
        }
    }

    public static class NodeViewTest
    {
        @Rule
        public TemporaryFolder tempFolder = new TemporaryFolder();

        @Test
        public void shouldDescribeObject() throws Exception
        {
            Path location = tempFolder.newFolder().toPath();
            KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());

            try (KafkaCachePartition partition = new KafkaCachePartition(location, config, "test", 0);
                    KafkaCachePartitionView partitionView = partition.acquire(KafkaCachePartitionView::new))
            {
                final NodeView sentinel = partitionView.sentinel();
                assertEquals("[NodeView] sentinel", sentinel.toString());
            }
        }
    }
}
