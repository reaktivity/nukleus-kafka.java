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

import java.util.HashMap;
import java.util.Map;

public final class KafkaCacheView extends KafkaCacheObjects.ReadOnly
{
    private final Map<String, KafkaCacheTopicView> topicsByName;
    private final KafkaCache cache;

    public KafkaCacheView(
        KafkaCache cache)
    {
        super(cache);
        this.cache = cache;
        this.topicsByName = new HashMap<>();
    }

    public String name()
    {
        return cache.name();
    }

    public KafkaCacheTopicView supplyTopicView(
        String name)
    {
        return topicsByName.computeIfAbsent(name, this::newTopicView);
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s", getClass().getSimpleName(), name());
    }

    @Override
    protected void onClosed()
    {
        topicsByName.values().forEach(KafkaCacheTopicView::close);
    }

    private KafkaCacheTopicView newTopicView(
        String name)
    {
        KafkaCacheTopic topic = cache.supplyTopic(name);
        return topic.acquire(KafkaCacheTopicView::new);
    }
}
