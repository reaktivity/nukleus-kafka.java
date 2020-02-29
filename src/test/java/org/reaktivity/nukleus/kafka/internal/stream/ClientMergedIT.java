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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_CLIENT_META_MAX_AGE_MILLIS;
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

public class ClientMergedIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("server", "org/reaktivity/specification/kafka/unmerged.fetch.v5.metadata.v5")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/kafka/streams/merged");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("kafka"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(8192)
        .configure(REAKTOR_BUFFER_SLOT_CAPACITY, 8192)
        .configure(ReaktorConfiguration.REAKTOR_DRAIN_ON_CLOSE, false)
        .configure(KAFKA_CLIENT_META_MAX_AGE_MILLIS, 1000)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Ignore("filtered")
    @Test
    @Specification({
        "${route}/client.merged/controller",
        "${client}/merged.filter.header/client",
        "${server}/unmerged.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMergedMessagesWithHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Specification({
        "${route}/client.merged/controller",
        "${client}/merged.filter.header.and.header/client",
        "${server}/unmerged.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMergedMessagesWithHeaderAndHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Specification({
        "${route}/client.merged/controller",
        "${client}/merged.filter.header.or.header/client",
        "${server}/unmerged.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMergedMessagesWithHeaderOrHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Specification({
        "${route}/client.merged/controller",
        "${client}/merged.filter.key/client",
        "${server}/unmerged.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMergedMessagesWithKeyFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Specification({
        "${route}/client.merged/controller",
        "${client}/merged.filter.key.and.header/client",
        "${server}/unmerged.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMergedMessagesWithKeyAndHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Ignore("filtered")
    @Test
    @Specification({
        "${route}/client.merged/controller",
        "${client}/merged.filter.key.or.header/client",
        "${server}/unmerged.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMergedMessagesWithKeyOrHeaderFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.merged/controller",
        "${client}/merged.filter.none/client",
        "${server}/unmerged.filter.none/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMergedMessagesWithNoFilter() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client.merged/controller",
        "${client}/merged.message.values/client",
        "${server}/unmerged.message.values/server"})
    @ScriptProperty("serverAddress \"nukleus://streams/target#0\"")
    public void shouldReceiveMergedMessageValues() throws Exception
    {
        k3po.finish();
    }
}
