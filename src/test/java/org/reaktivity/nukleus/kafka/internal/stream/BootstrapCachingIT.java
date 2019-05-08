/**
 * Copyright 2016-2019 The Reaktivity Project
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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_MESSAGE_CACHE_CAPACITY;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_MESSAGE_CACHE_PROACTIVE;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_TOPIC_BOOTSTRAP_ENABLED;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfigurationTest.KAFKA_MESSAGE_CACHE_PROACTIVE_NAME;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfigurationTest.KAFKA_READ_IDLE_TIMEOUT_NAME;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleusFactorySpi;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;
import org.reaktivity.reaktor.test.annotation.Configures;

public class BootstrapCachingIT
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
        .configure(KAFKA_TOPIC_BOOTSTRAP_ENABLED, true)
        .configure(KAFKA_MESSAGE_CACHE_CAPACITY, 1024L * 1024L)
        .configure(KAFKA_MESSAGE_CACHE_PROACTIVE, true)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${control}/route.ext.multiple.headers/client/controller",
        "${client}/compacted.messages.header.multiple.routes/client",
        "${server}/compacted.messages.header.multiple.values/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldCacheMessagesMatchingHeadersOnMultipleRoutesDuringBootstrap() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("FETCH_REQUEST_RECEIVED");

        // Wait for both routes to be added to the caching filter
        Thread.sleep(500);

        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");

        // ensure bootstrap is complete before client attaches
        while (reaktor.counter(KafkaNukleusFactorySpi.MESSAGE_CACHE_BUFFER_ACQUIRES) < 2L)
        {
            Thread.sleep(100);
        }

        k3po.notifyBarrier("CONNECT_CLIENT_ONE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.large.message.and.small/client",
        "${server}/compacted.messages.large.and.small/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://streams/target#0\"",
        "applicationConnectWindow1 \"2000\"",
        "applicationConnectWindow2 \"200\""
    })
    @Configure(name=KAFKA_READ_IDLE_TIMEOUT_NAME, value="200000")
    public void shouldReceiveCompactedFragmentedMessageAndFollowingFromCacheWhenNotSubscribedToKey() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("BOOTSTRAP_COMPLETE");
        k3po.notifyBarrier("CONNECT_CLIENT_ONE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.partial.message.aborted/client",
        "${server}/compacted.messages.large.then.small/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://streams/target#0\"",
        "secondMessageKey \"key1\""
    })
    public void shouldBeDetachedWhenPartiallyWrittenMessageIsReplacedWithNewValueInCache() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("BOOTSTRAP_COMPLETE");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.awaitBarrier("CLIENT_RECEIVED_PARTIAL_MESSAGE");
        k3po.notifyBarrier("WRITE_SECOND_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.partial.message.aborted.with.key/client",
        "${server}/compacted.messages.large.then.small/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://streams/target#0\"",
        "secondMessageKey \"key1\""
    })
    public void shouldNotThrowConcurrentModificationExceptionWhenDetached() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("BOOTSTRAP_COMPLETE");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.awaitBarrier("CLIENT_RECEIVED_PARTIAL_MESSAGE");
        k3po.notifyBarrier("WRITE_SECOND_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.partial.message.aborted/client",
        "${server}/compacted.messages.large.then.tombstone/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://streams/target#0\"",
        "secondMessageKey \"key1\""
    })
    public void shouldBeDetachedWhenPartiallyWrittenMessageIsReplacedWithTombstone() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("BOOTSTRAP_COMPLETE");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.awaitBarrier("CLIENT_RECEIVED_PARTIAL_MESSAGE");
        k3po.notifyBarrier("WRITE_SECOND_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages.advance.log.start.offset/client",
        "${server}/compacted.messages.advance.log.start.offset/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveCompactedMessagesAfterLogStartOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("LOG_START_OFFSET_UPDATED");
        k3po.notifyBarrier("CONNECT_CLIENT_ONE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages.multiple.nodes.historical/client",
        "${server}/compacted.messages.multiple.nodes.historical/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://streams/target#0\"",
        "applicationConnectWindow1 \"13\"",
        "applicationConnectWindow2 \"350\""
    })
    @Configures({
        @Configure(name=KAFKA_MESSAGE_CACHE_PROACTIVE_NAME, value="false"),
        @Configure(name=KAFKA_READ_IDLE_TIMEOUT_NAME, value="2000000")
    })
    public void shouldReceiveLargeHistoricalMessagesFromMultiplePartitions() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("BOOTSTRAP_COMPLETE_PARTITION_ZERO");
        k3po.awaitBarrier("BOOTSTRAP_COMPLETE_PARTITION_ONE");
        k3po.notifyBarrier("CONNECT_CLIENT_ONE");
        k3po.awaitBarrier("CLIENT_ONE_DATA_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.notifyBarrier("WRITE_HISTORICAL_PARTITION_ONE_RESPONSE");
        k3po.finish();
    }

}
