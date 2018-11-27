/**
 * Copyright 2016-2018 The Reaktivity Project
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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_FETCH_PARTITION_MAX_BYTES;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_MESSAGE_CACHE_CAPACITY;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class BootstrapIT
{
    private static final long DELETE_RETENTION_MS = 500;

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
        .counterValuesBufferCapacity(4096)
        .configure(KAFKA_FETCH_PARTITION_MAX_BYTES, 123000)
        .configure(KAFKA_MESSAGE_CACHE_CAPACITY, 0)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${server}/compacted.message/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "maxPartitionBytes 123000"
})
    public void shouldBootstrapTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${server}/compacted.offset.too.low.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBootstrapTopicStartingWithOffsetTooLow() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${server}/compacted.messages.multiple.nodes/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBootstrapTopicWithMultiplePartitionsOnMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.networks/client/controller",
        "${server}/compacted.message.multiple.networks/server"})
    @ScriptProperty({
        "networkAccept1 \"nukleus://target1/streams/kafka\"",
        "networkAccept2 \"nukleus://target2/streams/kafka\""})
    public void shouldBootstrapMultipleNetworks() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.topics/client/controller",
        "${server}/compacted.message.multiple.topics/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBootstrapMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ALL_METADATA_RESPONSES_WRITTEN");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_SECOND_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message/client",
        "${server}/compacted.bootstrap.uses.historical/server"})
    @ScriptProperty({"networkAccept \"nukleus://target/streams/kafka\"",
                     "offset \"4\""
    })
    public void shouldBootstrapWithHistoricalWhenClientSubscribesAtHigherOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("FIRST_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.awaitBarrier("CLIENT_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_LIVE_FETCH_RESPONSE");
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.awaitBarrier("HISTORICAL_FETCH_REQUEST_RECEIVED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_HISTORICAL_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/client/controller",
        "${client}/zero.offset.message/client",
        "${server}/zero.offset.message/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldNotBootstrapWhenTopicBootstrapIsEnabledButTopicIsNotCompacted() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/compacted.message/client",
        "${server}/compacted.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldNotBootstrapWhenRouteDoesNotNameATopic() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.bootstrap.historical.uses.cached.key.then.live/client",
        "${server}/compacted.bootstrap.historical.uses.cached.key.then.live/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBootstrapTopicAndUseCachedKeyOffsetThenLive() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.awaitBarrier("HISTORICAL_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("DELIVER_HISTORICAL_FETCH_RESPONSE");
        k3po.awaitBarrier("HISTORICAL_FETCH_RESPONSE_WRITTEN");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message/client",
        "${server}/compacted.tombstone.then.message/server"})
    @ScriptProperty({ "networkAccept \"nukleus://target/streams/kafka\"",
                      "messageOffset 12L",
                      })
    public void shouldExpireTombstoneKeyAndOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("RECEIVED_SECOND_FETCH_REQUEST");
        long schedulingMargin = 1000;
        long waitTimeForTombstoneExpiryToBeProcessed = DELETE_RETENTION_MS + schedulingMargin;
        Thread.sleep(waitTimeForTombstoneExpiryToBeProcessed);
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.awaitBarrier("CLIENT_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("DELIVER_SECOND_FETCH_RESPONSE");
        k3po.finish();
    }

    @Ignore("reaktivity/nukleus-kafka.java#122")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message.tombstone.message.same.key/client",
        "${server}/compacted.message.tombstone.message.same.key/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldNotExpireMessageFollowingTombstoneForSameKey() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_DONE");
        awaitWindowFromClient();
        long schedulingMargin = 1000;
        long waitTimeForTombstoneExpiryToBeProcessed = DELETE_RETENTION_MS + schedulingMargin;
        Thread.sleep(waitTimeForTombstoneExpiryToBeProcessed);

        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("DELIVER_HISTORICAL_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${metadata}/one.topic.error.unknown.topic/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldIssueWarningWhenBootstrapUnkownTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.message.connect.await/client",
        "${server}/zero.offset.message.topic.not.found.initially/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRequeryMetadataUntilFoundThenReceiveMessageAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("FIRST_METADATA_RESPONSE_WRITTEN");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    private void awaitWindowFromClient()
    {
        // Allow the reaktor process loop to process Window frame from client (and any other frames)
        try
        {
            Thread.sleep(200);
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
