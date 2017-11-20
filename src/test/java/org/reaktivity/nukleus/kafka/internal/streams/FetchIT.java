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
package org.reaktivity.nukleus.kafka.internal.streams;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

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

public class FetchIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("server", "org/reaktivity/specification/kafka/fetch.v5")
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

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/begin.ext.missing/client"})
    public void shouldRejectWhenBeginExtMissing() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${server}/zero.offset/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRequestMessagesAtZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.message/client",
        "${server}/zero.offset.message/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageAtZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages/client",
        "${server}/zero.offset.messages/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessagesAtZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messagesets/client",
        "${server}/zero.offset.messagesets/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageSetsAtZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.fanout/client",
        "${server}/zero.offset.messages.fanout/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldFanoutMessagesAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messagesets.fanout/client",
        "${server}/zero.offset.messagesets.fanout/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldFanoutMessageSetsAtZeroOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/nonzero.offset/client",
        "${server}/nonzero.offset/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRequestMessagesAtNonZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/nonzero.offset.message/client",
        "${server}/nonzero.offset.message/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageAtNonZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/nonzero.offset.messages/client",
        "${server}/nonzero.offset.messages/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessagesAtNonZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/distinct.offset.messagesets.fanout/client",
        "${server}/distinct.offset.messagesets.fanout/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldFanoutMessageSetsAtDistinctOffsets() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.finish();
    }
}
