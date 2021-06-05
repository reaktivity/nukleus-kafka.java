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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CLIENT_META_MAX_AGE_MILLIS;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CLIENT_PRODUCE_MAX_BYTES;
import static org.reaktivity.reaktor.ReaktorConfiguration.REAKTOR_BUFFER_SLOT_CAPACITY;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configuration;

public class ClientMergedIT
{
    private final K3poRule k3po = new K3poRule()
        .addScriptRoot("net", "org/reaktivity/specification/nukleus/kafka/streams/network/unmerged.p3.f5.d0.m5")
        .addScriptRoot("app", "org/reaktivity/specification/nukleus/kafka/streams/application/merged");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(REAKTOR_BUFFER_SLOT_CAPACITY, 8192)
        .configure(KAFKA_CLIENT_META_MAX_AGE_MILLIS, 1000)
        .configure(KAFKA_CLIENT_PRODUCE_MAX_BYTES, 116)
        .configurationRoot("org/reaktivity/specification/nukleus/kafka/config")
        .external("net#0")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.header/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.not.key/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithNotKeyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.not.header/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithNotHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.key.and.not.header/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithKeyAndNotHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.header.and.header/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.header.or.header/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.key/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithKeyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.key.and.header/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithKeyAndHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.key.or.header/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithKeyOrHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.none/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithNoFilter() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("RECEIVED_MESSAGE_B2");
        k3po.notifyBarrier("SEND_MESSAGE_A3");
        k3po.notifyBarrier("SEND_MESSAGE_B3");
        k3po.finish();
    }

    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.message.values/client",
        "${net}/unmerged.fetch.message.values/server"})
    public void shouldFetchMergedMessageValues() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.produce.message.values/client",
        "${net}/unmerged.produce.message.values/server"})
    public void shouldProduceMergedMessageValues() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.produce.message.values.dynamic/client",
        "${net}/unmerged.produce.message.values.dynamic/server"})
    public void shouldProduceMergedMessageValuesDynamic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.produce.message.values.dynamic.hashed/client",
        "${net}/unmerged.produce.message.values.dynamic.hashed/server"})
    public void shouldProduceMergedMessageValuesDynamicHashed() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.one/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeadersOneFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.one.empty/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeadersOneEmptyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.many/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeadersManyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.many.empty/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeadersManyEmptyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.skip.one/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeadersSkipOneFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.skip.two/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeadersSkipTwoFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Configuration("client.options.merged.json")
    @Specification({
        "${app}/merged.fetch.filter.headers.skip.many/client",
        "${net}/unmerged.fetch.filter.none/server"})
    public void shouldFetchMergedMessagesWithHeadersSkipManyFilter() throws Exception
    {
        k3po.finish();
    }
}
