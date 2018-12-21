/**
 * Copyright 2016-2018 The Reaktivity Project
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
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_TOPIC_BOOTSTRAP_ENABLED;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfigurationTest.KAFKA_READ_IDLE_TIMEOUT_NAME;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;
import org.reaktivity.reaktor.test.annotation.Configure;

public class MetadataIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("routeAnyTopic", "org/reaktivity/specification/nukleus/kafka/control/route")
            .addScriptRoot("metadata", "org/reaktivity/specification/kafka/metadata.v5")
            .addScriptRoot("server", "org/reaktivity/specification/kafka/fetch.v5")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/kafka/streams/fetch");

    private final TestRule timeout = new DisableOnDebug(new Timeout(15, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("kafka"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(4096)
        .configure(KAFKA_TOPIC_BOOTSTRAP_ENABLED, false)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/one.topic.leader.not.available.and.retry/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldRetryWhenLeaderNotAvailable() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/one.topic.unknown.error.abort.receive.abort.and.retry/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortReceiveAbortThenReconnectAndRetryWhenMetadataQueryGivesUnknownError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/one.topic.unknown.error.abort.receive.end.and.retry/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortReceiveEndThenReconnectAndRetryWhenMetadataQueryGivesUnknownError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/configs.response.unknown.error.abort.receive.abort.and.retry/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortReceiveAbortThenReconnectAndRetryWhenConfigsResponseGivesUnknownError() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/metadata.connection.aborted.and.reconnect/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReconnectAndContinueMetadataQueriesWhenBrokerConnectionIsAborted() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/metadata.connection.closed.and.reconnect/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReconnectAndContinueMetadataQueriesWhenBrokerIsEnded() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/metadata.connection.reset.and.reconnect/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReconnectAndContinueMetadataQueriesWhenBrokerConnectionIsReset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/one.topic.multiple.nodes/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldHandleMetadataResponseOneTopicOnMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/one.topic.multiple.nodes.and.replicas/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldHandleMetadataResponseOneTopicMultipleNodesAndReplicas() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/one.topic.multiple.partitions/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldHandleMetadataResponseOneTopicMultiplePartitionsSingleNode() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/one.topic.single.partition/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldHandleMetadataResponseOneTopicSinglePartition() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/metadata.incomplete.response.aborts/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortWhenMetadataResponseIsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/metadata.incomplete.response.broker.aborts/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortWhenMetadataResponseBrokerIsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/metadata.incomplete.response.after.brokers.aborts/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortWhenMetadataResponseAfterBrokersIsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/metadata.incomplete.response.topic.aborts/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortWhenMetadataResponseTopicIsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/metadata.incomplete.response.topic.partition.aborts/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortWhenMetadataResponseTopicPartitionIsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/describe.configs.incomplete.response.aborts/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortWhenDescribeConfigsResponseIsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/describe.configs.incomplete.response.resource.aborts/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortWhenDescribeConfigsResponseResourceIsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${metadata}/describe.configs.incomplete.response.resource.config.aborts/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldAbortWhenDescribeConfigsResponseResourceConfigIsIncomplete() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Configure(name=KAFKA_READ_IDLE_TIMEOUT_NAME, value="2000000")
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.multiple.topics/client",
        "${metadata}/unknown.and.known.topics/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldRepeatMetadataRequestsWhileTopicIsUnknownAndClientsAreAttachedWithoutBlockingOtherTopicUsage()
            throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("SECOND_UNKNOWN_TOPIC_METADATA_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("WRITE_SECOND_UNKNOWN_TOPIC_METADATA_RESPONSE");
        k3po.awaitBarrier("THIRD_UNKNOWN_TOPIC_METADATA_REQUEST_RECEIVED");
        k3po.notifyBarrier("DISCONNECT_CLIENT_ONE");
        k3po.notifyBarrier("WRITE_THIRD_UNKNOWN_TOPIC_METADATA_RESPONSE");
        k3po.notifyBarrier("CONNECT_CLIENT_THREE");
        k3po.finish();
    }
}
