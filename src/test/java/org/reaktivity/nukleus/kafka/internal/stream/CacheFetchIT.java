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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_RECONNECT_DELAY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_BUFFER_SLOT_CAPACITY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE;
import static org.reaktivity.reaktor.test.ReaktorRule.EXTERNAL_AFFINITY_MASK;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCache;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheTopic;
import org.reaktivity.reaktor.test.ReaktorRule;

public class CacheFetchIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("server", "org/reaktivity/specification/nukleus/kafka/streams/fetch")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/kafka/streams/fetch");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("kafka"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(REAKTOR_BUFFER_SLOT_CAPACITY, 8192)
        .configure(KAFKA_CACHE_SERVER_BOOTSTRAP, false)
        .configure(KAFKA_CACHE_SERVER_RECONNECT_DELAY, 0)
        .configure(KAFKA_CACHE_SEGMENT_BYTES, 1 * 1024 * 1024)
        .configure(KAFKA_CACHE_SEGMENT_INDEX_BYTES, 256 * 1024)
        .configure(REAKTOR_DRAIN_ON_CLOSE, false)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    private KafkaCachePartition partition;

    @Before
    public void initPartition()
    {
        final KafkaNukleus nukleus = reaktor.nukleus(KafkaNukleus.class);
        final KafkaCache cache = nukleus.supplyCache("kafka-cache#0");
        final KafkaCacheTopic topic = cache.supplyTopic("test");
        this.partition = topic.supplyFetchPartition(0);
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/topic.missing/client"})
    public void shouldRejectWhenTopicMissing() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/topic.not.routed/client"})
    public void shouldRejectWhenTopicNotRouted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/partition.unknown/client",
        "${server}/partition.unknown/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldRejectWhenPartitionUnknown() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/partition.not.leader/client",
        "${server}/partition.not.leader/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldRejectPartitionNotLeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/partition.offset/client",
        "${server}/partition.offset/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldRequestPartitionOffset() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/partition.offset.earliest/client",
        "${server}/partition.offset.earliest/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldRequestPartitionOffsetEarliest() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/partition.offset.latest/client",
        "${server}/partition.offset.latest/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldRequestPartitionOffsetLatest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.key/client",
        "${server}/message.key/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageKey() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.key.null/client",
        "${server}/message.key.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageKeyNull() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.key.with.value.null/client",
        "${server}/message.key.with.value.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageKeyWithValueNull() throws Exception
    {
        partition.append(3L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.key.with.value.distinct/client",
        "${server}/message.key.with.value.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageKeyWithValueDistinct() throws Exception
    {
        partition.append(4L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.key.with.header/client",
        "${server}/message.key.with.header/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageKeyWithHeader() throws Exception
    {
        partition.append(5L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.key.distinct/client",
        "${server}/message.key.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageKeyDistinct() throws Exception
    {
        partition.append(6L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value/client",
        "${server}/message.value/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValue() throws Exception
    {
        partition.append(10L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value.empty/client",
        "${server}/message.value.empty/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValueEmpty() throws Exception
    {
        partition.append(10L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value.null/client",
        "${server}/message.value.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValueNull() throws Exception
    {
        partition.append(11L);
        k3po.finish();
    }

    @Ignore("requires FIN dataEx from script")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value.10k/client",
        "${server}/message.value.10k/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValue10k() throws Exception
    {
        partition.append(12L);
        k3po.finish();
    }

    @Ignore("requires FIN dataEx from script")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value.100k/client",
        "${server}/message.value.100k/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValue100k() throws Exception
    {
        partition.append(12L);
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value.gzip/client",
        "${server}/message.value.gzip/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValueGzip() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value.snappy/client",
        "${server}/message.value.snappy/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValueSnappy() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value.lz4/client",
        "${server}/message.value.lz4/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValueLz4() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.value.distinct/client",
        "${server}/message.value.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageValueDistinct() throws Exception
    {
        partition.append(16L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.header/client",
        "${server}/message.header/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageHeader() throws Exception
    {
        partition.append(20L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.header.null/client",
        "${server}/message.header.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageHeaderNull() throws Exception
    {
        partition.append(21L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.headers.distinct/client",
        "${server}/message.headers.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageHeadersDistinct() throws Exception
    {
        partition.append(22L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/message.headers.repeated/client",
        "${server}/message.headers.repeated/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageHeadersRepeated() throws Exception
    {
        partition.append(23L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.none/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithNoFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.key/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithKeyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.key.and.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithKeyAndHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.key.or.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithKeyOrHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.header.and.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_4");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.header.or.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.key.and.header.or.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithKeyAndHeaderOrHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.key.or.header.and.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithKeyOrHeaderAndHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.delta/json.patch/controller",
        "${client}/filter.none.json.patch/client",
        "${server}/filter.none.json/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveJsonPatchMessagesWithNoFilter() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.delta/json.patch/controller",
        "${client}/filter.header.json.patch/client",
        "${server}/filter.none.json/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveJsonPatchMessagesWithHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache.delta/json.patch/controller",
        "${client}/filter.key.and.header.json.patch/client",
        "${server}/filter.none.json/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveJsonPatchMessagesWithKeyAndHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.not.key/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithNotKeyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.not.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithNotHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.key.and.not.header/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithKeyAndNotHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.headers.one/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersOneFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.headers.one.empty/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersOneEmptyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.headers.many/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersManyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.headers.many.empty/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersManyEmptyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.headers.skip.one/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersSkipOneFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.headers.skip.two/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersSkipTwoFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/cache/controller",
        "${client}/filter.headers.skip.many/client",
        "${server}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesWithHeadersSkipManyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }
}
