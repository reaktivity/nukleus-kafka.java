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
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_BUFFER_SLOT_CAPACITY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

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

public class ClientProduceIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("server", "org/reaktivity/specification/kafka/produce.v3")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/kafka/streams/produce");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("kafka"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(REAKTOR_DRAIN_ON_CLOSE, false)
        .configure(REAKTOR_BUFFER_SLOT_CAPACITY, 8192)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/topic.missing/client"})
    public void shouldRejectWhenTopicMissing() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/topic.not.routed/client"})
    public void shouldRejectWhenTopicNotRouted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/partition.unknown/client",
        "${server}/partition.unknown/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldRejectWhenPartitionUnknown() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/partition.not.leader/client",
        "${server}/partition.not.leader/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldRejectPartitionNotLeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.key/client",
        "${server}/message.key/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.key.null/client",
        "${server}/message.key.null/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageKeyNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.key.with.value.null/client",
        "${server}/message.key.with.value.null/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageKeyWithValueNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.key.with.value.distinct/client",
        "${server}/message.key.with.value.distinct/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageKeyWithValueDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.key.with.header/client",
        "${server}/message.key.with.header/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageKeyWithHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.key.distinct/client",
        "${server}/message.key.distinct/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageKeyDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value/client",
        "${server}/message.value/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValue() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value.null/client",
        "${server}/message.value.null/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValueNull() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value.10k/client",
        "${server}/message.value.10k/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValue10k() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value.100k/client",
        "${server}/message.value.100k/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValue100k() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value.gzip/client",
        "${server}/message.value.gzip/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValueGzip() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value.snappy/client",
        "${server}/message.value.snappy/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValueSnappy() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value.lz4/client",
        "${server}/message.value.lz4/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValueLz4() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value.distinct/client",
        "${server}/message.value.distinct/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValueDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.header/client",
        "${server}/message.header/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.header.null/client",
        "${server}/message.header.null/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageHeaderNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.headers.distinct/client",
        "${server}/message.headers.distinct/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageHeadersDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.headers.repeated/client",
        "${server}/message.headers.repeated/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageHeadersRepeated() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/message.value.repeated/client",
        "${server}/message.value.repeated/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldSendMessageValueRepeated() throws Exception
    {
        k3po.finish();
    }
}
