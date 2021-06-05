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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_RECONNECT_DELAY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_BUFFER_SLOT_CAPACITY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE;

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
import org.reaktivity.reaktor.test.annotation.Configuration;

public class CacheFetchIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/kafka/streams/application/fetch");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
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
        .configurationRoot("org/reaktivity/specification/nukleus/kafka/config")
        .external("app#1")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    private KafkaCachePartition partition;

    @Before
    public void initPartition()
    {
        final KafkaNukleus nukleus = reaktor.nukleus(KafkaNukleus.class);
        final KafkaCache cache = nukleus.supplyCache("default.cache#0");
        final KafkaCacheTopic topic = cache.supplyTopic("test");
        this.partition = topic.supplyFetchPartition(0);
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/topic.missing/client"})
    public void shouldRejectWhenTopicMissing() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.when.topic.json")
    @Specification({
        "${app}/topic.not.routed/client"})
    public void shouldRejectWhenTopicNotRouted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/partition.unknown/client",
        "${app}/partition.unknown/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldRejectWhenPartitionUnknown() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/partition.not.leader/client",
        "${app}/partition.not.leader/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldRejectPartitionNotLeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/partition.offset/client",
        "${app}/partition.offset/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldRequestPartitionOffset() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/partition.offset.earliest/client",
        "${app}/partition.offset.earliest/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldRequestPartitionOffsetEarliest() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/partition.offset.latest/client",
        "${app}/partition.offset.latest/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldRequestPartitionOffsetLatest() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key/client",
        "${app}/message.key/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageKey() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.null/client",
        "${app}/message.key.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageKeyNull() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.with.value.null/client",
        "${app}/message.key.with.value.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageKeyWithValueNull() throws Exception
    {
        partition.append(3L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.with.value.distinct/client",
        "${app}/message.key.with.value.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageKeyWithValueDistinct() throws Exception
    {
        partition.append(4L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.with.header/client",
        "${app}/message.key.with.header/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageKeyWithHeader() throws Exception
    {
        partition.append(5L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.distinct/client",
        "${app}/message.key.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageKeyDistinct() throws Exception
    {
        partition.append(6L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value/client",
        "${app}/message.value/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValue() throws Exception
    {
        partition.append(10L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.empty/client",
        "${app}/message.value.empty/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValueEmpty() throws Exception
    {
        partition.append(10L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.null/client",
        "${app}/message.value.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValueNull() throws Exception
    {
        partition.append(11L);
        k3po.finish();
    }

    @Ignore("requires FIN dataEx from script")
    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.10k/client",
        "${app}/message.value.10k/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValue10k() throws Exception
    {
        partition.append(12L);
        k3po.finish();
    }

    @Ignore("requires FIN dataEx from script")
    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.100k/client",
        "${app}/message.value.100k/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValue100k() throws Exception
    {
        partition.append(12L);
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.gzip/client",
        "${app}/message.value.gzip/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValueGzip() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.snappy/client",
        "${app}/message.value.snappy/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValueSnappy() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.lz4/client",
        "${app}/message.value.lz4/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValueLz4() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.distinct/client",
        "${app}/message.value.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageValueDistinct() throws Exception
    {
        partition.append(16L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.header/client",
        "${app}/message.header/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageHeader() throws Exception
    {
        partition.append(20L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.header.null/client",
        "${app}/message.header.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageHeaderNull() throws Exception
    {
        partition.append(21L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.headers.distinct/client",
        "${app}/message.headers.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageHeadersDistinct() throws Exception
    {
        partition.append(22L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.headers.repeated/client",
        "${app}/message.headers.repeated/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessageHeadersRepeated() throws Exception
    {
        partition.append(23L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.none/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithNoFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.key/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithKeyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.key.and.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithKeyAndHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.key.or.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithKeyOrHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.header.and.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_4");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.header.or.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.key.and.header.or.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithKeyAndHeaderOrHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.key.or.header.and.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithKeyOrHeaderAndHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.delta.type.json")
    @Specification({
        "${app}/filter.none.json.patch/client",
        "${app}/filter.none.json/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveJsonPatchMessagesWithNoFilter() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.delta.type.json")
    @Specification({
        "${app}/filter.header.json.patch/client",
        "${app}/filter.none.json/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveJsonPatchMessagesWithHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.options.delta.type.json")
    @Specification({
        "${app}/filter.key.and.header.json.patch/client",
        "${app}/filter.none.json/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveJsonPatchMessagesWithKeyAndHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.not.key/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithNotKeyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.not.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithNotHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.key.and.not.header/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithKeyAndNotHeaderFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.headers.one/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeadersOneFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.headers.one.empty/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeadersOneEmptyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.headers.many/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeadersManyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.headers.many.empty/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeadersManyEmptyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.headers.skip.one/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeadersSkipOneFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.headers.skip.two/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeadersSkipTwoFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/filter.headers.skip.many/client",
        "${app}/filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldReceiveMessagesWithHeadersSkipManyFilter() throws Exception
    {
        partition.append(1L);
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_2");
        k3po.notifyBarrier("SEND_MESSAGE_3");
        k3po.finish();
    }
}
