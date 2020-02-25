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

import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;

public class KafkaCacheTopicViewTest
{
    @Test
    public void shouldClosePartitions() throws Exception
    {
        KafkaConfiguration config = new KafkaConfiguration();
        KafkaCacheTopic topic = new KafkaCacheTopic(config, "test");
        KafkaCacheTopicView topicView = topic.acquire(KafkaCacheTopicView::new);

        KafkaCachePartition partition = topic.supplyPartition(0);
        KafkaCachePartitionView partitionView = topicView.supplyPartitionView(0);

        topic.close();
        topicView.close();

        assert partitionView.closed();
        assert partition.closed();
    }

    @Test
    public void shouldDescribeObject() throws Exception
    {
        KafkaConfiguration config = new KafkaConfiguration();

        try (KafkaCacheTopic topic = new KafkaCacheTopic(config, "test");
                KafkaCacheTopicView topicView = topic.acquire(KafkaCacheTopicView::new))
        {
            assertEquals("test", topicView.name());
            assertEquals("[KafkaCacheTopicView] test", topicView.toString());
        }
    }
}
