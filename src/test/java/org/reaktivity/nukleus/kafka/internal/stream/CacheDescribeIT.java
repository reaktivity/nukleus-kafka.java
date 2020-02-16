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
package org.reaktivity.nukleus.kafka.internal.stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SEGMENT_BYTES;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SEGMENT_INDEX_BYTES;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_BOOTSTRAP;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_RECONNECT;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;

public class CacheDescribeIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route")
            .addScriptRoot("routeEx", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/kafka/streams/describe")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/kafka/streams/describe");

    private final TestRule timeout = new DisableOnDebug(new Timeout(15, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("kafka"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(KAFKA_CACHE_SERVER_BOOTSTRAP, false)
        .configure(KAFKA_CACHE_SERVER_RECONNECT, false)
        .configure(ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE, false)
        .configure(KAFKA_CACHE_SEGMENT_BYTES, 1 * 1024 * 1024)
        .configure(KAFKA_CACHE_SEGMENT_INDEX_BYTES, 256 * 1024)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/topic.unknown/client" })
    public void shouldRejectWhenTopicUnknown() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeEx}/cache/controller",
        "${client}/topic.config.info/client",
        "${server}/topic.config.info/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveTopicConfigInfo() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeEx}/cache/controller",
        "${client}/topic.config.info.changed/client",
        "${server}/topic.config.info.changed/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveTopicConfigInfoChanged() throws Exception
    {
        k3po.finish();
    }
}
