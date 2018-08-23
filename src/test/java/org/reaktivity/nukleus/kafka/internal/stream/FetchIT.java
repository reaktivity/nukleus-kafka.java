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

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;

public class FetchIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("routeAnyTopic", "org/reaktivity/specification/nukleus/kafka/control/route")
            .addScriptRoot("control", "org/reaktivity/specification/nukleus/kafka/control")
            .addScriptRoot("server", "org/reaktivity/specification/kafka/fetch.v5")
            .addScriptRoot("metadata", "org/reaktivity/specification/kafka/metadata.v5")
            .addScriptRoot("client", "org/reaktivity/specification/nukleus/kafka/streams/fetch");

    private final TestRule timeout = new DisableOnDebug(new Timeout(10, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
        .nukleus("kafka"::equals)
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .configure(KafkaConfiguration.TOPIC_BOOTSTRAP_ENABLED, "false")
        .configure(KafkaConfiguration.MESSAGE_CACHE_CAPACITY_PROPERTY, "0")
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Ignore("BEGIN vs RESET read order not yet guaranteed to match write order")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/begin.ext.missing/client"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRejectWhenBeginExtMissing() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/invalid.topic.name/client",
        "${metadata}/one.topic.error.invalid.topic/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRejectWhenTopicNameContainsDisallowedCharacters() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/invalid.fetch.key.and.multiple.offsets/client"})
    public void shouldRejectInvalidBeginExWithFetchKeyAndMultipleOffsets() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/invalid.missing.fetch.key/client"})
    public void shouldRejectInvalidBeginExWithMissingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/invalid.more.than.one.fetch.key.hash/client"})
    public void shouldRejectInvalidBeginExWithMoreThanOneFetchKeyHash() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.headers/client/controller",
        "${client}/invalid.no.route.matching.headers/client"})
    public void shouldRejectWhenBeginHeadersDoNotMatchHeadersOnRoutes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/topic.name.not.equals.route.ext/client"})
    @ScriptProperty("routedTopicName \"not_test\"")
    public void shouldRejectTopicNameNotEqualToRoutedTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/unknown.topic.name/client",
        "${metadata}/two.topics.error.unknown.topic/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRejectWhenTopicIsUnknown() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.delivers.deleted.messages/client",
        "${server}/compacted.delivers.deleted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedDeletedMessages() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_UNSUBSCRIBED");
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.delivers.compacted.messages/client",
        "${server}/compacted.delivers.compacted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessages() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_UNSUBSCRIBED");
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.header.messages.and.tombstone/client",
        "${server}/compacted.header.matches.removed.in.subsequent.response/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageAndTombstoneWithHeaderRemovedInSubsequentResponse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.header.messages.and.tombstone/client",
        "${server}/compacted.header.matches.then.updated/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageAndTombstoneWithHeaderUpdated() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.header.repeated.tombstone/client",
        "${server}/compacted.header.repeated.tombstone/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageAndTombstoneWithMultivaluedHeaderUpdated() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("DELIVER_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.after.unsubscribe/client",
        "${server}/compacted.historical.uses.cached.key.after.unsubscribe/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessageUsingCachedKeyAfterAllClientsUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_UNSUBSCRIBED");
        k3po.awaitBarrier("SECOND_LIVE_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.then.latest.offset/client",
        "${server}/compacted.historical.uses.cached.key.then.latest.offset/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessagesWithUncachedKeyUsingLatestOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_THREE_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.then.live/client",
        "${server}/compacted.historical.uses.cached.key.then.live/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessageUsingCachedKeyOffsetThenCatchUpToLiveStream() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_RECEIVED_FIRST_MESSAGE");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_RECEIVED_FIRST_MESSAGE");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.then.live.after.null.message/client",
        "${server}/compacted.historical.uses.cached.key.then.live.after.null.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessagesFromLiveStreamAfterCachedKeyRemovedByNullMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification(
    {"${route}/client/controller",
            "${client}/compacted.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/client",
            "${server}/compacted.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessagesFromLiveStreamAfterOffsetTooEarlyAndCachedKeyRemovedByNullMessage()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.then.zero.offset/client",
        "${server}/compacted.historical.uses.cached.key.then.zero.offset/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessagesWithUncachedKeyUsingZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message/client",
        "${server}/compacted.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessage() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.notifyBarrier("CONNECT_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message.fanout/client",
        "${server}/compacted.message.delayed.describe.response/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessageWithMessageKeyAndLastOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("DESCRIBE_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.notifyBarrier("DELIVER_DESCRIBE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages/client",
        "${server}/compacted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedAdjacentMessages() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages.tombstone.repeated/client",
        "${server}/compacted.messages.tombstone.repeated/server"})
    @ScriptProperty({ "networkAccept \"nukleus://target/streams/kafka\"",
                      "newTimestamp 0L" })
    public void shouldReceiveCompactedAdjacentRepeatedTombstoneMessages() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("ROUTED_CLIENT");
        k3po.awaitBarrier("RECEIVED_NEXT_FETCH_REQUEST");
        k3po.notifyBarrier("SUBSCRIBE_CLIENT");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message.subscribed.to.key/client",
        "${server}/compacted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveLastMatchingMessageOnlyWhenSubscribedByKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message.subscribed.to.key/client",
        "${server}/compacted.messages/server"})
    @ScriptProperty({"networkAccept \"nukleus://target/streams/kafka\"",
                     "applicationConnectWindow 25",
                     "applicationConnectPadding 10"})
    public void shouldReceiveLastMatchingMessageSkippingMessageLargerThanWindow() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.header/client/controller",
        "${client}/compacted.messages.header/client",
        "${server}/compacted.messages.header/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldMatchRouteAndReceiveCompactedMessagesFilteredByHeaderOnBegin() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.header/client/controller",
        "${client}/compacted.messages.headers/client",
        "${server}/compacted.messages.headers/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldMatchRouteAndReceiveCompactedMessagesFilteredByHeadersOnBegin() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.header/client/controller",
        "${client}/compacted.messages.header/client",
        "${server}/compacted.messages.header/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessagesFilteredByHeaderOnRoute() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages.historical/client",
        "${server}/compacted.messages.historical/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedHistoricalMessages() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages.multiple.nodes/client",
        "${server}/compacted.messages.multiple.nodes/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveCompactedMessagesFromMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Ignore("May fail due to unpredicable read ordering of window frames from clients and first fetch response")
    @Test
    @Specification({
        "${control}/route.ext.multiple.topics/client/controller",
        "${client}/compacted.message.multiple.topics/client",
        "${server}/compacted.message.multiple.topics/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessagesFromMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.awaitBarrier("CLIENT_THREE_CONNECTED");
        k3po.awaitBarrier("ALL_METADATA_RESPONSES_WRITTEN");
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.notifyBarrier("WRITE_SECOND_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/unspecified.offset/client",
        "${server}/compacted.zero.offset/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldStartFetchingAtOffsetZeroForCompactedTopicClientOffsetUnspecified() throws Exception
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
        k3po.notifyBarrier("SERVER_DELIVER_HISTORICAL_RESPONSE");
        k3po.awaitBarrier("CLIENT_ONE_RECEIVED_THIRD_MESSAGE");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_THREE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/distinct.offset.messagesets.fanout/client",
        "${server}/distinct.offset.messagesets.fanout/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldFanoutMessageSetsAtDistinctOffsetsWithoutDeliveringLiveMessageOutOfOrder() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_THREE");
        k3po.awaitBarrier("CLIENT_ONE_RECEIVED_THIRD_MESSAGE");
        k3po.notifyBarrier("SERVER_DELIVER_HISTORICAL_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/distinct.offsets.message.fanout/client",
        "${server}/distinct.offsets.message.fanout/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldHandleParallelSubscribesAtDistinctOffsets() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.awaitBarrier("SERVER_LIVE_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("SERVER_DELIVER_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fanout.with.historical.message/client",
        "${server}/fanout.with.historical.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldFanoutUsingHistoricalConnection() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fanout.with.historical.messages/client",
        "${server}/fanout.with.historical.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldFanoutDiscardingHistoricalMessageToJoinLiveStream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fanout.with.slow.consumer/client",
        "${server}/fanout.with.slow.consumer/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldFanoutWithSlowConsumer() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_TWO_RECEIVED_SECOND_MESSAGE");
        awaitWindowFromClient();
        k3po.notifyBarrier("SERVER_DELIVER_LIVE_RESPONSE_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.and.hash.code.picks.partition.zero/client",
        "${server}/fetch.key.and.hash.code.picks.partition.zero/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageUsingFetchKeyAndExplicitHashCode() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.and.no.key.messages/client",
        "${server}/fetch.key.and.no.key.multiple.partitions/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageOnSubscribesWithAndWithoutKeyFromMultiplePartitions() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.and.no.key.unsubscribe/client",
        "${server}/fetch.key.and.no.key.multiple.partitions.unsubscribe/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldNotGiveAssertionErrorWhenUnsubscribeLeavingConsumerOnAnotherPartition() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_TWO_HAS_WRITTEN_RESET");
        k3po.notifyBarrier("CLIENT_TWO_UNSUBSCRIBED");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.default.partitioner.picks.partition.one/client",
        "${server}/fetch.key.default.partitioner.picks.partition.one/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageUsingFetchKeyAndDefaultPartitioner() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.historical.does.not.use.cached.key/client",
        "${server}/fetch.key.historical.does.not.use.cached.key/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageFromNonCompactedTopicWithoutCachingKeyOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.messages/client",
        "${server}/fetch.key.multiple.matches.flow.controlled/server"})
    @ScriptProperty({"networkAccept \"nukleus://target/streams/kafka\"",
                     "applicationConnectWindow 15"})
    public void shouldReceiveMultipleMessagesMatchingFetchKeyFlowControlled() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.message/client",
        "${server}/fetch.key.multiple.record.batches.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingFetchKeyWithLastOffsetWithMultipleRecordBatches() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.no.messages/client",
        "${server}/fetch.key.multiple.record.batches.no.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveNoMessagesMatchingFetchKeyWithMultipleRecordBatches() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.no.messages/client",
        "${server}/fetch.key.no.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveNoMessagesMatchingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.three.messages/client",
        "${server}/fetch.key.three.matches.flow.controlled/server"})
    @ScriptProperty({"networkAccept \"nukleus://target/streams/kafka\"",
                     "applicationConnectWindow 15"})
    public void shouldReceiveMessagesThreeMatchingFetchKeyFlowControlled() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.nonzero.offset.message/client",
        "${server}/fetch.key.nonzero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingFetchKeyFirstNonZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.nonzero.offset.too.early.message/client",
        "${server}/fetch.key.nonzero.offset.too.early.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldUseEarliestAvailableOffsetIfGreaterThanRequestedOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.message/client",
        "${server}/fetch.key.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingFetchKeyFirst() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.unspecified.offset.message/client",
        "${server}/fetch.key.high.water.mark.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveLiveMessageMatchingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.message/client",
        "${server}/fetch.key.zero.offset.last.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingFetchKeyLast() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.messages/client",
        "${server}/fetch.key.zero.offset.multiple.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMultipleMessagesMatchingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.messages.historical/client",
        "${server}/fetch.key.zero.offset.multiple.matches.historical/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMultipleHistoricalMessagesMatchingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.and.fetch.key.zero.offset.message/client",
        "${server}/header.and.fetch.key.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingFetchKeyAndHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.empty.value.message/client",
        "${server}/header.first.message.has.empty.header.value/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingEmptyHeaderValue() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.zero.offset.message/client",
        "${server}/header.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingHeaderFirst() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.zero.offset.message/client",
        "${server}/header.zero.offset.last.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingHeaderLast() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.zero.offset.messages/client",
        "${server}/header.zero.offset.multiple.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMultipleMessagesMatchingHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.zero.offset.repeated/client",
        "${server}/header.zero.offset.repeated/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingAnyOccurrenceOfARepeatedHeader() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/headers.and.fetch.key.zero.offset.message/client",
        "${server}/headers.and.fetch.key.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingFetchKeyAndHeaders() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/headers.zero.offset.messages.historical/client",
        "${server}/headers.zero.offset.multiple.matches.historical/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveHistoricalMessagesMatchingHeaders() throws Exception
    {
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
        "${client}/offset.too.early.message/client",
        "${server}/offset.too.early.message/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRefetchUsingReportedFirstOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/offset.too.early.multiple.nodes/client",
        "${server}/offset.too.early.multiple.nodes/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRefetchUsingReportedFirstOffsetOnMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/offset.too.early.multiple.topics/client",
        "${server}/offset.too.early.multiple.topics/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRefetchUsingReportedFirstOffsetOnMultipleTopics() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        k3po.notifyBarrier("WRITE_FIRST_METADATA_RESPONSE");
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("WRITE_SECOND_DESCRIBE_CONFIGS_RESPONSE");
        k3po.awaitBarrier("CLIENT_TWO_SUBSCRIBED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.ends.with.deleted.record/client",
        "${server}/record.batch.ends.with.deleted.record/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldUseLastOffsetFromRecordBatch() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.truncated/client",
        "${server}/record.batch.ends.with.truncated.record/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageWithTruncatedRecord() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.ends.with.truncated.record.length/client",
        "${server}/record.batch.ends.with.truncated.record.length/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageWithTruncatedRecordLength() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.truncated/client",
        "${server}/record.batch.truncated/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageRecordSetEndsWithTruncatedRecordBatch() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.truncated/client",
        "${server}/record.batch.truncated.at.record.boundary/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageRecordBatchTruncatedOnRecordBoundary() throws Exception
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
        "${client}/zero.offset/client",
        "${server}/zero.offset.no.messages/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldNotSkipZeroLenthRecordBatchIfAtHighWatermark() throws Exception
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
        k3po.start();
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/no.offsets.message/client",
        "${server}/zero.offset.message/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageAtZeroOffsetWhenEmptyOffsetsArray() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.and.reset/client",
        "${server}/zero.offset.message/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldHandleFetchResponseAfterUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("SUBSCRIBED");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.and.reset/client",
        "${server}/zero.offset.messages.multiple.partitions/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldHandleFetchResponseMultiplePartitionsAfterUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("SUBSCRIBED");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.message/client",
        "${server}/zero.offset.message.single.partition.multiple.nodes/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldNotFetchFromNodeWithNoPartitions() throws Exception
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
        "${client}/unspecified.offset/client",
        "${server}/high.water.mark.offset/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldAttachAtHighWaterMarkOffsetForStreamingTopic() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/unspecified.offset.multiple.topics/client",
        "${server}/high.water.mark.offset.multiple.topics/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldAttachAtHighWaterMarkOffsetForStreamingTopics() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_THREE_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fanout.with.historical.messages/client",
        "${server}/historical.connection.aborted/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReconnectRequeryPartitionMetadataAndContinueReceivingMessagesWhenHistoricalFetchConnectionIsAborted()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fanout.with.historical.messages/client",
        "${server}/historical.connection.closed/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReconnectAndReceiveMessagesWhenHistoricalFetchConnectionIsClosed() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fanout.with.historical.messages/client",
        "${server}/historical.connection.reset/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReconnectRequeryPartitionMetadataAndContinueReceivingMessagesWhenHistoricalFetchConnectionIsReset()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.large.then.small.messages.multiple.partitions/client",
        "${server}/header.large.exceeding.window.then.small.messages.multiple.partitions/server" })
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"200\""
    })
    public void shouldAdvanceFetchOffsetForNonMatchingMessagesOnPartitionDifferentFromFragmentedMessage()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.large.then.small.messages.multiple.partitions/client",
        "${server}/header.large.equals.window.then.small.messages.multiple.partitions/server" })
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"266\""
    })
    public void shouldAdvanceFetchOffsetForNonMatchingMessagesOnPartitionDifferentFromMessageExhaustingWindow()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/large.then.small.messages.multiple.partitions/client",
        "${server}/large.equals.window.then.small.messages.multiple.partitions/server" })
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"266\""
    })
    public void shouldDeliverLargeMessageFillingWindowThenSmallMessagesFromMultiplePartitions()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/large.then.small.messages.multiple.partitions/client",
        "${server}/large.exceeding.window.then.small.messages.multiple.partitions/server" })
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"200\""
    })
    public void shouldNotDeliverMessageFromPartitionDifferentFromFragmentedMessageUntilFragmentedFullyWritten()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/large.then.small.messages.multiple.partitions/client",
        "${server}/large.then.small.other.partition.first.in.next.response/server" })
    @ScriptProperty({
        "networkAccept \"nukleus://target/streams/kafka\"",
        "applicationConnectWindow \"200\""
    })
    public void shouldNotDeliverSecondFragmentFromADifferentMessageFromAnotherPartition()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.topic.recreated/client",
        "${server}/live.fetch.broker.restarted.with.recreated.topic/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessagesAcrossBrokerRestartWithRecreatedTopic() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_RECEIVED_FIRST_MESSAGE");
        k3po.notifyBarrier("SHUTDOWN_BROKER");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.message.two.topics.multiple.partitions/client",
        "${server}/live.fetch.connection.aborted/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReconnectRequeryPartitionMetadataAndContinueReceivingMessagesWhenLiveFetchConnectionIsAborted()
            throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages/client",
        "${server}/live.fetch.connection.closed/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReconnectAndReceiveMessagesWhenLiveFetchConnectionIsClosed() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages/client",
        "${server}/live.fetch.connection.closed.then.reset/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRefreshMetadataAndReceiveMessagesWhenLiveFetchConnectionIsClosedThenResetThenReconnected() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.messages.multiple.partitions/client",
        "${server}/live.fetch.connection.fails.during.metadata.refresh/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldHandleFetchConnectionFailureDuringMetadataRefresh() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("DESCRIBE_CONFIGS_REQUEST_RECEIVED");
        k3po.notifyBarrier("ABORT_CONNECTION_TWO");
        k3po.awaitBarrier("CONNECTION_TWO_ABORTED");

        //  Give time for the broker 2 to be removed from the metadata
        awaitWindowFromClient();

        k3po.notifyBarrier("WRITE_DESCRIBE_CONFIGS_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.message.two.topics.multiple.partitions/client",
        "${server}/live.fetch.connection.reset/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReconnectRequeryPartitionMetadataAndContinueReceivingMessagesWhenLiveFetchConnectionIsReset()
            throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.message.two.topics/client",
        "${server}/live.fetch.error.recovered/server" })
    @ScriptProperty({
    "networkAccept \"nukleus://target/streams/kafka\"",
    "errorCode 3s"
    })
    public void shouldContinueReceivingMessagesWhenTopicFetchResponseIsErrorCode3DuringKafkaRestart() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.message.two.topics/client",
        "${server}/live.fetch.error.recovered/server" })
    @ScriptProperty({
    "networkAccept \"nukleus://target/streams/kafka\"",
    "errorCode 6s"
    })
    public void shouldContinueReceivingMessagesAfterLeadershipElection() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.messages.partition.added/client",
        "${server}/live.fetch.error.recovered.partition.added/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldBeDetachedReattachAndContinueReceivingMessagesWhenPartitionCountChanges() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.message.two.topics/client",
        "${server}/live.fetch.error.then.metadata.error/server" })
    @ScriptProperty({
    "networkAccept \"nukleus://target/streams/kafka\""
    })
    public void shouldHandleFetchErrorFollowedByMetadataRefreshError() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/zero.offset.message.two.topics.one.detached/client",
        "${server}/live.fetch.topic.not.found.permanently/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldDetachClientsWhenTopicIsPermanentlyDeleted() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FIRST_FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("WRITE_FIRST_FETCH_RESPONSE");
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
            "${client}/zero.offset.messages.group.budget/client",
            "${server}/zero.offset.messages.group.budget/server" })
    @ScriptProperty({"networkAccept \"nukleus://target/streams/kafka\"",
                     "applicationConnectWindow 24"})
    public void shouldFanoutMessagesAtZeroOffsetUsingGroupBudget() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        awaitWindowFromClient();
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
            "${route}/client/controller",
            "${client}/zero.offset.messages.group.budget.reset/client",
            "${server}/zero.offset.messages.group.budget/server" })
    @ScriptProperty({"networkAccept \"nukleus://target/streams/kafka\"",
            "applicationConnectWindow 24"})
    public void shouldFanoutMessagesAtZeroOffsetUsingGroupBudgetReset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_ONE");
        awaitWindowFromClient();
        k3po.notifyBarrier("SERVER_DELIVER_RESPONSE_TWO");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.multiple.partitions/client",
        "${server}/zero.offset.messages.multiple.nodes/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessagesFromMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.multiple.partitions/client",
        "${server}/zero.offset.messages.multiple.partitions/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessagesFromMultiplePartitions() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Ignore("order of RESET vs BEGIN/DATA/ABORT/END not guaranteed")
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.multiple.partitions.partition.1/client",
        "${server}/zero.offset.messages.multiple.partitions.partition.1/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldCleanUpStateWhenUnsubscribeAfterReceiveMessageFromSecondPartition() throws Exception
    {
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

    private void awaitWindowFromClient()
    {
        // Allow the reaktor process loop to process Window frame from client (and any other frames)
        try
        {
            Thread.sleep(200);
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
