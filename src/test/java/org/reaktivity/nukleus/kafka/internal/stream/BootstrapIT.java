/**
 * Copyright 2016-2017 The Reaktivity Project
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
        .counterValuesBufferCapacity(1024)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${server}/ktable.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBootstrapTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${server}/ktable.messages.multiple.nodes/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBootstrapTopicWithMultiplePartitionsOnMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.networks/client/controller",
        "${server}/ktable.message.multiple.networks/server"})
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
        "${server}/ktable.message.multiple.topics/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBootstrapMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("SECOND_METADATA_RESPONSE_WRITTEN");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route/client/controller",
        "${client}/no.offsets.message/client",
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
        "${client}/ktable.message/client",
        "${server}/ktable.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldNotBootstrapWhenRouteDoesNotNameATopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/ktable.bootstrap.historical.uses.cached.key.then.live/client",
        "${server}/ktable.bootstrap.historical.uses.cached.key.then.live/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBootstrapTopicAndUseCachedKeyOffsetThenLive() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.awaitBarrier("HISTORICAL_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("DELIVER_HISTORICAL_FETCH_RESPONSE");
        k3po.awaitBarrier("HISTORICAL_FETCH_RESPONSE_WRITTEN");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

}
