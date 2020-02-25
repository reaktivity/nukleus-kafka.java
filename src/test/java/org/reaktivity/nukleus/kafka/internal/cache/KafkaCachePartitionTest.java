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

public class KafkaCachePartitionTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

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
}
