/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.kafka.internal.stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SEGMENT_BYTES;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SEGMENT_INDEX_BYTES;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_BOOTSTRAP;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_RECONNECT_DELAY;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;

public class CacheDescribeIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/kafka/streams/application/describe");

    private final TestRule timeout = new DisableOnDebug(new Timeout(15, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(KAFKA_CACHE_SERVER_BOOTSTRAP, false)
        .configure(KAFKA_CACHE_SERVER_RECONNECT_DELAY, 0)
        .configure(KAFKA_CACHE_SEGMENT_BYTES, 1 * 1024 * 1024)
        .configure(KAFKA_CACHE_SEGMENT_INDEX_BYTES, 256 * 1024)
        .configurationRoot("org/reaktivity/specification/nukleus/kafka/config")
        .external("app#1")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/topic.unknown/client" })
    public void shouldRejectWhenTopicUnknown() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/topic.config.info/client",
        "${app}/topic.config.info/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveTopicConfigInfo() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/topic.config.info.changed/client",
        "${app}/topic.config.info.changed/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveTopicConfigInfoChanged() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_TOPIC_CONFIG");
        k3po.notifyBarrier("CHANGE_TOPIC_CONFIG");
        k3po.finish();
    }
}
