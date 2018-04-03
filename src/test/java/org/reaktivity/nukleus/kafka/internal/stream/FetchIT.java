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
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class FetchIT
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
        .counterValuesBufferCapacity(1024)
        .clean();

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(timeout);

    @Rule
    public ExpectedException expected = ExpectedException.none();

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
        "${metadata}/one.topic.error.unknown.topic/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldRejectWhenTopicIsUnknown() throws Exception
    {
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
        "${client}/fetch.key.no.offsets.message/client",
        "${server}/fetch.key.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageMatchingFetchKeyFirstWithZeroLengthOffsetsArray() throws Exception
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
        "${client}/ktable.delivers.compacted.deleted.messages/client",
        "${server}/ktable.delivers.compacted.deleted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableCompactedDeletedMessages() throws Exception
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
        "${client}/ktable.delivers.compacted.messages/client",
        "${server}/ktable.delivers.compacted.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableCompactedMessages() throws Exception
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
        "${client}/ktable.historical.uses.cached.key.after.unsubscribe/client",
        "${server}/ktable.historical.uses.cached.key.after.unsubscribe/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableMessageUsingCachedKeyAfterAllClientsUnsubscribe() throws Exception
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
        "${client}/ktable.historical.uses.cached.key.then.latest.offset/client",
        "${server}/ktable.historical.uses.cached.key.then.latest.offset/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableMessagesWithUncachedKeyUsingLatestOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_THREE_CONNECTED");
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/ktable.historical.uses.cached.key.then.live/client",
        "${server}/ktable.historical.uses.cached.key.then.live/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableMessageUsingCachedKeyOffsetThenCatchUpToLiveStream() throws Exception
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
        "${client}/ktable.historical.uses.cached.key.then.live.after.null.message/client",
        "${server}/ktable.historical.uses.cached.key.then.live.after.null.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableMessagesFromLiveStreamAfterCachedKeyRemovedByNullMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification(
    {"${route}/client/controller",
            "${client}/ktable.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/client",
            "${server}/ktable.historical.uses.cached.key.then.live.after.offset.too.early.and.null.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableMessagesFromLiveStreamAfterOffsetTooEarlyAndCachedKeyRemovedByNullMessage()
            throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/ktable.historical.uses.cached.key.then.zero.offset/client",
        "${server}/ktable.historical.uses.cached.key.then.zero.offset/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableMessagesWithUncachedKeyUsingZeroOffset() throws Exception
    {
        k3po.start();
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/ktable.message/client",
        "${server}/ktable.message/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKtableMessage() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/ktable.message.fanout/client",
        "${server}/ktable.message.delayed.describe.response/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKtableMessageWithMessageKeyAndLastOffset() throws Exception
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
        "${client}/ktable.messages/client",
        "${server}/ktable.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKtableMessages() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/ktable.messages.header.multiple.matches/client",
        "${server}/ktable.messages.header.multiple.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKtableMessagesFilteredByHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/ktable.messages.historical/client",
        "${server}/ktable.messages.historical/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableHistoricalMessages() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/ktable.messages.multiple.nodes/client",
        "${server}/ktable.messages.multiple.nodes/server"})
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveKTableMessagesFromMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${server}/live.fetch.abort.and.reconnect/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReconnectOnAbortOnLiveFetchConnection() throws Exception
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
        "${client}/record.batch.ends.with.truncated.record/client",
        "${server}/record.batch.ends.with.truncated.record/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReceiveMessageWithTruncatedRecord() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.ends.with.truncated.record.then.stall/client",
        "${server}/record.batch.ends.with.truncated.record.then.stall/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReportFetchesStalled() throws Exception
    {
        expected.expectMessage("stalled fetch");
        k3po.start();
        k3po.awaitBarrier("FINAL_FETCH_RESPONSE_WRITTEN");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.message/client",
        "${server}/live.fetch.reset.reconnect.and.message/server" })
    @ScriptProperty("networkAccept \"nukleus://target/streams/kafka\"")
    public void shouldReconnectOnResetOnLiveConnectionAndReceiveMessage() throws Exception
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
}
