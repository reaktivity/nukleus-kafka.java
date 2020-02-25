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

public class KafkaCacheViewTest
{
    @Test
    public void shouldCloseTopics() throws Exception
    {
        KafkaConfiguration config = new KafkaConfiguration();
        KafkaCache cache = new KafkaCache(config, "cache");
        KafkaCacheView cacheView = cache.acquire(KafkaCacheView::new);

        KafkaCacheTopic topic = cache.supplyTopic("test");
        KafkaCacheTopicView topicView = cacheView.supplyTopicView("test");

        cache.close();
        cacheView.close();

        assert topicView.closed();
        assert topic.closed();
    }

    @Test
    public void shouldDescribeName() throws Exception
    {
        KafkaConfiguration config = new KafkaConfiguration();

        try (KafkaCache cache = new KafkaCache(config, "test");
                KafkaCacheView cacheView = cache.acquire(KafkaCacheView::new))
        {
            assertEquals("test", cacheView.name());
        }
    }

    @Test
    public void shouldDescribeObject() throws Exception
    {
        KafkaConfiguration config = new KafkaConfiguration();

        try (KafkaCache cache = new KafkaCache(config, "test");
                KafkaCacheView cacheView = cache.acquire(KafkaCacheView::new))
        {
            assertEquals("[KafkaCacheView] test", cacheView.toString());
        }
    }
}
