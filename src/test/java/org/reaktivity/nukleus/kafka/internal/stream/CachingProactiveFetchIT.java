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
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_MESSAGE_CACHE_CAPACITY;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_MESSAGE_CACHE_PROACTIVE;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_READ_IDLE_TIMEOUT;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_TOPIC_BOOTSTRAP_ENABLED;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.kafka.internal.test.KafkaCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class CachingProactiveFetchIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("routeAnyTopic", "org/reaktivity/specification/nukleus/kafka/control/route")
            .addScriptRoot("control", "org/reaktivity/specification/nukleus/kafka/control")
            .addScriptRoot("server", "org/reaktivity/specification/kafka/fetch.v5")
            .addScriptRoot("metadata", "org/reaktivity/specification/kafka/metadata.v5")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/kafka/streams/fetch");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("kafka"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(KAFKA_TOPIC_BOOTSTRAP_ENABLED, false)
        .configure(KAFKA_MESSAGE_CACHE_PROACTIVE, true)
        .configure(KAFKA_MESSAGE_CACHE_CAPACITY, 1024L * 1024L)
        .configure(KAFKA_READ_IDLE_TIMEOUT, 86400)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    private final KafkaCountersRule counters = new KafkaCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(counters).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages.one.per.key/client",
        "${server}/compacted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    // Behavior is different with message cache: better compaction, guaranteed only one message per key
    public void shouldReceiveCompactedHistoricalMessagesFromCacheWhenProactiveMessageCachingIsEnabled() throws Exception
    {
        k3po.finish();
        assertEquals(2, counters.cacheHits());
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.delivers.compacted.messages/client",
        "${server}/compacted.delivers.compacted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveCompactedMessages() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_UNSUBSCRIBED");
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.delivers.deleted.messages/client",
        "${server}/compacted.delivers.deleted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveCompactedDeletedMessagesFromCache() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_UNSUBSCRIBED");
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.header.message.multiple.clients/client",
        "${server}/compacted.header.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveHistoricalMessageMatchingHeaderFromCache() throws Exception
    {
        k3po.finish();
        assertEquals(2, counters.cacheHits());
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message.subscribed.to.key/client",
        "${server}/compacted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveLastMatchingMessageOnlyFromCacheWhenSubscribedByKey() throws Exception
    {
        k3po.finish();
    }

}
