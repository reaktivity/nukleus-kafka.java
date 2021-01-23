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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_BUFFER_SLOT_CAPACITY;
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
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;

public class CacheMergedIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/kafka/streams/merged")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/kafka/streams/merged");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("kafka"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(16384)
        .configure(REAKTOR_BUFFER_SLOT_CAPACITY, 8192)
        .configure(KAFKA_CACHE_SERVER_BOOTSTRAP, false)
        .configure(KAFKA_CACHE_SEGMENT_BYTES, 1 * 1024 * 1024)
        .configure(KAFKA_CACHE_SEGMENT_INDEX_BYTES, 256 * 1024)
        .configure(ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE, false)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.header/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.header.with.compaction/client",
        "${server}/unmerged.fetch.filter.none.with.compaction/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithHeaderFilterAfterCompaction() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.not.key/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithNotKeyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.not.header/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithNotHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.key.and.not.header/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithKeyAndNotHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.header.and.header/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.header.or.header/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.key/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithKeyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.key.and.header/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithKeyAndHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.key.or.header/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithKeyOrHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.filter.none/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_B2");
        k3po.notifyBarrier("SEND_MESSAGE_A3");
        k3po.notifyBarrier("SEND_MESSAGE_B3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.message.values/client",
        "${server}/unmerged.fetch.message.values/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedMessageValues() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CHANGING_PARTITION_COUNT");
        Thread.sleep(200); // allow A1, B1, A2, B2 to be merged
        k3po.notifyBarrier("CHANGED_PARTITION_COUNT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.partition.leader.changed/client",
        "${server}/unmerged.fetch.partition.leader.changed/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedPartitionLeaderChanged() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CHANGING_PARTITION_LEADER");
        Thread.sleep(200);
        k3po.notifyBarrier("CHANGED_PARTITION_LEADER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.partition.leader.aborted/client",
        "${server}/unmerged.fetch.partition.leader.aborted/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedPartitionLeaderAborted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.partition.offsets.earliest/client",
        "${server}/unmerged.fetch.partition.offsets.earliest/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedPartitionOffsetsEarliest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.partition.offsets.earliest.overflow/client",
        "${server}/unmerged.fetch.partition.offsets.earliest/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldFetchMergedPartitionOffsetsEarliestOverflow() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.produce.message.values/client",
        "${server}/unmerged.produce.message.values/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldProduceMergedMessageValues() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.produce.message.values.dynamic/client",
        "${server}/unmerged.produce.message.values.dynamic/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldProduceMergedMessageValuesDynamic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.produce.message.values.dynamic.hashed/client",
        "${server}/unmerged.produce.message.values.dynamic.hashed/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldProduceMergedMessageValuesDynamicHashed() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.produce.message.flags.incomplete/client",
        "${server}/unmerged.produce.message.flags.incomplete/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldProduceMergedMessageFlagsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.server.sent.close/client",
        "${server}/unmerged.fetch.server.sent.close/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.server.sent.close.with.message/client",
        "${server}/unmerged.fetch.server.sent.close.with.message/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchCloseWithMessage() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE");
        k3po.notifyBarrier("CLOSE_UNMERGED_FETCH_REPLY");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.server.sent.abort/client",
        "${server}/unmerged.fetch.server.sent.abort/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchAbort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.server.sent.abort.with.message/client",
        "${server}/unmerged.fetch.server.sent.abort.with.message/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchAbortWithMessage() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE");
        k3po.notifyBarrier("ABORT_UNMERGED_FETCH_REPLY");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.server.sent.abort/client",
        "${server}/unmerged.fetch.server.sent.reset/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchReset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.merged/controller",
        "${client}/merged.fetch.server.sent.abort.with.message/client",
        "${server}/unmerged.fetch.server.sent.reset.and.abort.with.message/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchResetWithMessage() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE");
        k3po.notifyBarrier("RESET_UNMERGED_FETCH_INITIAL");
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/merged.fetch.filter.headers.one/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersOneFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/merged.fetch.filter.headers.one.empty/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersOneEmptyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/merged.fetch.filter.headers.many/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersManyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/merged.fetch.filter.headers.many.empty/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersManyEmptyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/merged.fetch.filter.headers.skip.one/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersSkipOneFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/merged.fetch.filter.headers.skip.two/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersSkipTwoFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/merged.fetch.filter.headers.skip.many/client",
        "${server}/unmerged.fetch.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersSkipManyFilter() throws Exception
    {
        k3po.finish();
    }
}
