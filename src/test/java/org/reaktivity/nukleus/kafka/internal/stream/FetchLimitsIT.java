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
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;

public class FetchLimitsIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("routeAnyTopic", "org/reaktivity/specification/nukleus/kafka/control/route")
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
        .clean()
        .configure(KafkaConfiguration.TOPIC_BOOTSTRAP_ENABLED, "false")
        .configure(ReaktorConfiguration.BUFFER_SLOT_CAPACITY_PROPERTY, 256)
        .configure(KafkaConfiguration.FETCH_MAX_BYTES_PROPERTY, 355)
        .configure(KafkaConfiguration.FETCH_PARTITION_MAX_BYTES_PROPERTY, 355);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.large.message.subscribed.to.key/client",
        "${server}/compacted.messages.large.and.small/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveLargeCompactedMessageWhenSubscribedToKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.large.message.subscribed.to.key/client",
        "${server}/compacted.messages.large.and.small.repeated/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"200\""
    })
    public void shouldReceiveCompactedFragmentedMessageWhenSubscribedToKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.partial.message.aborted/client",
        "${server}/zero.offset.messages.large.and.small.then.large.missing/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBeDettachedWhenPartiallyDeliveredMessageNoLongerAvailable() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.large.message/client",
        "${server}/zero.offset.messages.large.and.small/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageExceedingBufferSlotCapacity() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.large.message/client",
        "${server}/zero.offset.messages.large.and.small.repeated/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"200\""
    })
    public void shouldReceiveMessageExceedingInitialWindow() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.large.message/client",
        "${server}/zero.offset.first.record.batch.large.requires.3.fetches/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"100\""
    })
    public void shouldReceiveLargeMessageRequiringThreeDataFrames() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.message/client",
        "${server}/message.size.exceeds.max.partition.bytes/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"355\"",
        "messageOffset \"2\""
    })
    public void shouldSkipRecordBatchExceedingMaxPartitionBytes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.multiple.partitions/client",
        "${server}/record.set.size.zero.record.too.large/server"})
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "offsetMessage2 2"
    })
    public void shouldSkipRecordLargerThanFetchPartitionMaxBytes() throws Exception
    {
        k3po.finish();
    }
}
