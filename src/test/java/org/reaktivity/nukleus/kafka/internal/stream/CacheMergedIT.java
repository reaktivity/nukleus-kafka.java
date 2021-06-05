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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_BUFFER_SLOT_CAPACITY;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.ReaktorConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;
import org.reaktivity.reaktor.test.annotation.Configure;

public class CacheMergedIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/kafka/streams/application/merged");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(16384)
        .configure(REAKTOR_BUFFER_SLOT_CAPACITY, 8192)
        .configure(KAFKA_CACHE_SERVER_BOOTSTRAP, false)
        .configure(KAFKA_CACHE_SEGMENT_BYTES, 1 * 1024 * 1024)
        .configure(KAFKA_CACHE_SEGMENT_INDEX_BYTES, 256 * 1024)
        .configure(ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE, false)
        .configurationRoot("org/reaktivity/specification/nukleus/kafka/config")
        .external("app#1")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.header/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.header.with.compaction/client",
        "${app}/unmerged.fetch.filter.none.with.compaction/server"})
    public void shouldFetchMergedMessagesWithHeaderFilterAfterCompaction() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.not.key/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithNotKeyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.not.header/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithNotHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.key.and.not.header/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithKeyAndNotHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Configuration("cache.options.merged.json")
    @Test
    @Specification({
        "${app}/merged.fetch.filter.header.and.header/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Configuration("cache.options.merged.json")
    @Test
    @Specification({
        "${app}/merged.fetch.filter.header.or.header/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.key/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithKeyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.key.and.header/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithKeyAndHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.key.or.header/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithKeyOrHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.none/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_B2");
        k3po.notifyBarrier("SEND_MESSAGE_A3");
        k3po.notifyBarrier("SEND_MESSAGE_B3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.message.values/client",
        "${app}/unmerged.fetch.message.values/server"})
    public void shouldFetchMergedMessageValues() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CHANGING_PARTITION_COUNT");
        Thread.sleep(200); // allow A1, B1, A2, B2 to be merged
        k3po.notifyBarrier("CHANGED_PARTITION_COUNT");
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.partition.leader.changed/client",
        "${app}/unmerged.fetch.partition.leader.changed/server"})
    public void shouldFetchMergedPartitionLeaderChanged() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CHANGING_PARTITION_LEADER");
        Thread.sleep(200);
        k3po.notifyBarrier("CHANGED_PARTITION_LEADER");
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.partition.leader.aborted/client",
        "${app}/unmerged.fetch.partition.leader.aborted/server"})
    public void shouldFetchMergedPartitionLeaderAborted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.partition.offsets.earliest/client",
        "${app}/unmerged.fetch.partition.offsets.earliest/server"})
    public void shouldFetchMergedPartitionOffsetsEarliest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.partition.offsets.earliest.overflow/client",
        "${app}/unmerged.fetch.partition.offsets.earliest/server"})
    public void shouldFetchMergedPartitionOffsetsEarliestOverflow() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.produce.message.values/client",
        "${app}/unmerged.produce.message.values/server"})
    public void shouldProduceMergedMessageValues() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.produce.message.values.dynamic/client",
        "${app}/unmerged.produce.message.values.dynamic/server"})
    public void shouldProduceMergedMessageValuesDynamic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.produce.message.values.dynamic.hashed/client",
        "${app}/unmerged.produce.message.values.dynamic.hashed/server"})
    public void shouldProduceMergedMessageValuesDynamicHashed() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.produce.message.flags.incomplete/client",
        "${app}/unmerged.produce.message.flags.incomplete/server"})
    public void shouldProduceMergedMessageFlagsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.server.sent.close/client",
        "${app}/unmerged.fetch.server.sent.close/server"})
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchClose() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.server.sent.close.with.message/client",
        "${app}/unmerged.fetch.server.sent.close.with.message/server"})
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchCloseWithMessage() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE");
        k3po.notifyBarrier("CLOSE_UNMERGED_FETCH_REPLY");
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.server.sent.abort/client",
        "${app}/unmerged.fetch.server.sent.abort/server"})
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchAbort() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.server.sent.abort.with.message/client",
        "${app}/unmerged.fetch.server.sent.abort.with.message/server"})
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchAbortWithMessage() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE");
        k3po.notifyBarrier("ABORT_UNMERGED_FETCH_REPLY");
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.server.sent.abort/client",
        "${app}/unmerged.fetch.server.sent.reset/server"})
    @Configure(name = KAFKA_CACHE_SERVER_RECONNECT_DELAY_NAME, value = "0")
    public void shouldCloseMergedOnUnmergedFetchReset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.server.sent.abort.with.message/client",
        "${app}/unmerged.fetch.server.sent.reset.and.abort.with.message/server"})
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
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.one/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldReceiveMessagesWithHeadersOneFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.one.empty/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldReceiveMessagesWithHeadersOneEmptyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.many/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldReceiveMessagesWithHeadersManyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.many.empty/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldReceiveMessagesWithHeadersManyEmptyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.skip.one/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldReceiveMessagesWithHeadersSkipOneFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.skip.two/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldReceiveMessagesWithHeadersSkipTwoFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("requires k3po parallel reads")
    @Test
    @Configuration("cache.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.skip.many/client",
        "${app}/unmerged.fetch.filter.none/server"})
    public void shouldReceiveMessagesWithHeadersSkipManyFilter() throws Exception
    {
        k3po.finish();
    }
}
