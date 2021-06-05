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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_BOOTSTRAP;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CACHE_SERVER_RECONNECT_DELAY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_BUFFER_SLOT_CAPACITY;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE;

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
import org.reaktivity.reaktor.test.annotation.Configuration;

public class CacheProduceIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/kafka/streams/application/produce");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(REAKTOR_BUFFER_SLOT_CAPACITY, 8192)
        .configure(KAFKA_CACHE_SERVER_BOOTSTRAP, false)
        .configure(KAFKA_CACHE_SERVER_RECONNECT_DELAY, 0)
        .configure(REAKTOR_DRAIN_ON_CLOSE, false)
        .configurationRoot("org/reaktivity/specification/nukleus/kafka/config")
        .external("app#1")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

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
        "${app}/message.key/client",
        "${app}/message.key/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.null/client",
        "${app}/message.key.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageKeyNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.with.value.null/client",
        "${app}/message.key.with.value.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageKeyWithValueNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.with.value.distinct/client",
        "${app}/message.key.with.value.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageKeyWithValueDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.with.header/client",
        "${app}/message.key.with.header/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageKeyWithHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.key.distinct/client",
        "${app}/message.key.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageKeyDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value/client",
        "${app}/message.value/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageValue() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.null/client",
        "${app}/message.value.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageValueNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.10k/client",
        "${app}/message.value.10k/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageValue10k() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.100k/client",
        "${app}/message.value.100k/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageValue100k() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO")
    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.gzip/client",
        "${app}/message.value.gzip/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageValueGzip() throws Exception
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
    public void shouldSendMessageValueSnappy() throws Exception
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
    public void shouldSendMessageValueLz4() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.distinct/client",
        "${app}/message.value.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageValueDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.header/client",
        "${app}/message.header/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.header.null/client",
        "${app}/message.header.null/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageHeaderNull() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.headers.distinct/client",
        "${app}/message.headers.distinct/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageHeadersDistinct() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.headers.repeated/client",
        "${app}/message.headers.repeated/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageHeadersRepeated() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("cache.json")
    @Specification({
        "${app}/message.value.repeated/client",
        "${app}/message.value.repeated/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/app#1\"")
    public void shouldSendMessageValueRepeated() throws Exception
    {
        k3po.finish();
    }
}
