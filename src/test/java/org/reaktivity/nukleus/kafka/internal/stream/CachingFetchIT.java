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
import static org.junit.Assert.assertEquals;
import static org.junit.rules.RuleChain.outerRule;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_MESSAGE_CACHE_CAPACITY;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_MESSAGE_CACHE_PROACTIVE;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_READ_IDLE_TIMEOUT;
import static org.reaktivity.nukleus.kafka.internal.KafkaConfiguration.KAFKA_TOPIC_BOOTSTRAP_ENABLED;
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
import org.reaktivity.nukleus.kafka.internal.test.KafkaCountersRule;
import org.reaktivity.reaktor.test.ReaktorRule;

public class CachingFetchIT
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
        .counterValuesBufferCapacity(8192)
        .configure(KAFKA_TOPIC_BOOTSTRAP_ENABLED, false)
        .configure(KAFKA_MESSAGE_CACHE_CAPACITY, 1024L * 1024L)
        .configure(KAFKA_MESSAGE_CACHE_PROACTIVE, false)
        .configure(KAFKA_READ_IDLE_TIMEOUT, 86400)
        .affinityMask("target#0", EXTERNAL_AFFINITY_MASK)
        .clean();

    private final KafkaCountersRule counters = new KafkaCountersRule(reaktor);

    @Rule
    public final TestRule chain = outerRule(reaktor).around(k3po).around(counters).around(timeout);

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/begin.ext.missing/client"})
    public void shouldRejectWhenBeginExtMissing() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/invalid.topic.name/client",
        "${metadata}/one.topic.error.invalid.topic/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
        "${route}/client/controller",
        "${client}/compacted.historical.empty.message/client",
        "${server}/compacted.empty.message/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveCompactedEmptyMessageFromCacheWhenSubscribedToKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.header.message.multiple.clients/client",
        "${server}/compacted.header.first.matches.repeated/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveHistoricalMessageMatchingHeaderFirstFromCache() throws Exception
    {
        k3po.finish();
        assertEquals(1, counters.cacheHits());
        assertEquals(1, counters.cacheMisses());
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.header.messages.and.tombstone/client",
        "${server}/compacted.header.matches.removed.in.subsequent.response/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageAndTombstoneWithHeaderRemovedInSubsequentResponse() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.header.messages.and.tombstone/client",
        "${server}/compacted.header.matches.then.updated/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageAndTombstoneWithHeaderUpdated() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.after.unsubscribe/client",
        "${server}/compacted.historical.uses.cached.key.after.unsubscribe/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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

    // No historical fetch with message cache active
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.then.latest.offset/client",
        "${server}/compacted.historical.uses.cached.key.then.latest.offset.no.historical/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveCompactedMessagesWithUncachedKeyUsingLatestOffset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_THREE_CONNECTED");
        awaitWindowFromClient();
        k3po.notifyBarrier("DELIVER_SECOND_LIVE_RESPONSE");
        k3po.finish();
        assertEquals(1, counters.cacheHits());
    }

    // No historical fetch with message cache active
    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.then.live/client",
        "${server}/compacted.historical.uses.cached.key.then.live.no.historical/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveCompactedMessageUsingCachedKeyOffsetThenCatchUpToLiveStream() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_RECEIVED_FIRST_MESSAGE");
        k3po.notifyBarrier("CONNECT_CLIENT_TWO");
        k3po.awaitBarrier("CLIENT_TWO_RECEIVED_FIRST_MESSAGE");
        k3po.notifyBarrier("DELIVER_FINAL_LIVE_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.historical.uses.cached.key.then.zero.offset/client",
        "${server}/compacted.historical.uses.zero.offset/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveCompactedMessagesWithCachedKeyUsingZeroOffset() throws Exception
    {
        k3po.finish();
        assertEquals(2, counters.cacheHits());
        assertEquals(0, counters.cacheMisses());
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.message/client",
        "${server}/compacted.message/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
        "${client}/compacted.messages.header/client",
        "${server}/compacted.messages.header/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveCompactedMessagesFilteredByHeaderOnBegin() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages.one.per.key/client",
        "${server}/compacted.messages.live/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    // Behavior is different with message cache: better compaction, guaranteed only one message per key
    public void shouldReceiveCompactedHistoricalMessagesFromCacheWhenOriginallyReceivedAsLiveMessages() throws Exception
    {
        k3po.finish();
        assertEquals(2, counters.cacheHits());
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/compacted.messages.multiple.nodes/client",
        "${server}/compacted.messages.multiple.nodes/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
        "${client}/distinct.offset.messagesets.fanout/client",
        "${server}/distinct.offset.messagesets.fanout/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldFanoutUsingHistoricalConnection() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fanout.with.historical.messages/client",
        "${server}/fanout.with.historical.messages/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldFanoutDiscardingHistoricalMessageToJoinLiveStream() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fanout.with.slow.consumer/client",
        "${server}/fanout.with.slow.consumer/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageUsingFetchKeyAndExplicitHashCode() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.and.no.key.messages/client",
        "${server}/fetch.key.and.no.key.multiple.partitions/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageOnSubscribesWithAndWithoutKeyFromMultiplePartitions() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.and.no.key.unsubscribe/client",
        "${server}/fetch.key.and.no.key.multiple.partitions.unsubscribe/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageUsingFetchKeyAndDefaultPartitioner() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.historical.does.not.use.cached.key/client",
        "${server}/fetch.key.historical.does.not.use.cached.key/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageFromNonCompactedTopicWithoutCachingKeyOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.messages/client",
        "${server}/fetch.key.multiple.matches.flow.controlled/server"})
    @ScriptProperty({"networkAccept \"nukleus://streams/target#0\"",
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageMatchingFetchKeyWithLastOffsetWithMultipleRecordBatches() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.no.messages/client",
        "${server}/fetch.key.multiple.record.batches.no.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveNoMessagesMatchingFetchKeyWithMultipleRecordBatches() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.no.messages/client",
        "${server}/fetch.key.no.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveNoMessagesMatchingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.three.messages/client",
        "${server}/fetch.key.three.matches.flow.controlled/server"})
    @ScriptProperty({"networkAccept \"nukleus://streams/target#0\"",
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageMatchingFetchKeyFirstNonZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.nonzero.offset.too.low.message/client",
        "${server}/fetch.key.nonzero.offset.too.low.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldUseEarliestAvailableOffsetIfGreaterThanRequestedOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.message/client",
        "${server}/fetch.key.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageMatchingFetchKeyFirst() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.unspecified.offset.message/client",
        "${server}/fetch.key.high.water.mark.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveLiveMessageMatchingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.message/client",
        "${server}/fetch.key.zero.offset.last.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageMatchingFetchKeyLast() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.messages/client",
        "${server}/fetch.key.zero.offset.multiple.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMultipleMessagesMatchingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/fetch.key.zero.offset.messages.historical/client",
        "${server}/fetch.key.zero.offset.multiple.matches.historical/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMultipleHistoricalMessagesMatchingFetchKey() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.and.fetch.key.zero.offset.message/client",
        "${server}/header.and.fetch.key.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageMatchingFetchKeyAndHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.zero.offset.message/client",
        "${server}/header.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageMatchingHeaderFirst() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.zero.offset.message/client",
        "${server}/header.zero.offset.last.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageMatchingHeaderLast() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/header.zero.offset.messages/client",
        "${server}/header.zero.offset.multiple.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMultipleMessagesMatchingHeader() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/headers.and.fetch.key.zero.offset.message/client",
        "${server}/headers.and.fetch.key.zero.offset.first.matches/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageMatchingFetchKeyAndHeaders() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/headers.zero.offset.messages.historical/client",
        "${server}/headers.zero.offset.multiple.matches.historical/server"})
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveHistoricalMessagesMatchingHeaders() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/nonzero.offset/client",
        "${server}/nonzero.offset/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldRequestMessagesAtNonZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/nonzero.offset.message/client",
        "${server}/nonzero.offset.message/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageAtNonZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/nonzero.offset.messages/client",
        "${server}/nonzero.offset.messages/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesAtNonZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/offset.too.low.message/client",
        "${server}/offset.too.low.message/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldRefetchUsingReportedFirstOffset() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/offset.too.low.multiple.nodes/client",
        "${server}/offset.too.low.multiple.nodes/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldRefetchUsingReportedFirstOffsetOnMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeAnyTopic}/client/controller",
        "${client}/offset.too.low.multiple.topics/client",
        "${server}/offset.too.low.multiple.topics/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldUseLastOffsetFromRecordBatch() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.truncated/client",
        "${server}/record.batch.ends.with.truncated.record/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageWithTruncatedRecord() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.truncated/client",
        "${server}/record.batch.truncated/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageRecordSetEndsWithTruncatedRecordBatch() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/record.batch.truncated/client",
        "${server}/record.batch.truncated.at.record.boundary/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageRecordBatchTruncatedOnRecordBoundary() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${server}/zero.offset/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldRequestMessagesAtZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset/client",
        "${server}/zero.offset.no.messages/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldNotSkipZeroLenthRecordBatchIfAtHighWatermark() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.message/client",
        "${server}/zero.offset.message/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageAtZeroOffset() throws Exception
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldHandleFetchResponseAfterUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("DO_CLIENT_RESET");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.and.reset/client",
        "${server}/zero.offset.messages.multiple.partitions/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldHandleFetchResponseMultiplePartitionsAfterUnsubscribe() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("FETCH_REQUEST_RECEIVED");
        k3po.notifyBarrier("DO_CLIENT_RESET");
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.message/client",
        "${server}/zero.offset.message.single.partition.multiple.nodes/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldNotFetchFromNodeWithNoPartitions() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages/client",
        "${server}/zero.offset.messages/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesAtZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messagesets/client",
        "${server}/zero.offset.messagesets/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessageSetsAtZeroOffset() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.fanout/client",
        "${server}/zero.offset.messages.fanout/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
            "${client}/zero.offset.messages.shared.budget/client",
            "${server}/zero.offset.messages.shared.budget/server" })
    @ScriptProperty({"networkAccept \"nukleus://streams/target#0\"",
                     "applicationConnectWindow 24"})
    public void shouldFanoutMessagesAtZeroOffsetUsingSharedBudget() throws Exception
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
            "${client}/zero.offset.messages.shared.budget.reset/client",
            "${server}/zero.offset.messages.shared.budget/server" })
    @ScriptProperty({"networkAccept \"nukleus://streams/target#0\"",
            "applicationConnectWindow 24"})
    public void shouldFanoutMessagesAtZeroOffsetUsingSharedBudgetReset() throws Exception
    {
        k3po.start();
        k3po.awaitBarrier("CLIENT_ONE_CONNECTED");
        k3po.awaitBarrier("CLIENT_TWO_CONNECTED");
        awaitWindowFromClient();
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
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesFromMultipleNodes() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.multiple.partitions/client",
        "${server}/zero.offset.messages.multiple.partitions/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldReceiveMessagesFromMultiplePartitions() throws Exception
    {
        k3po.start();
        k3po.notifyBarrier("WRITE_FETCH_RESPONSE");
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messages.multiple.partitions.partition.1/client",
        "${server}/zero.offset.messages.multiple.partitions.partition.1/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
    public void shouldCleanUpStateWhenUnsubscribeAfterReceiveMessageFromSecondPartition() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${client}/zero.offset.messagesets.fanout/client",
        "${server}/zero.offset.messagesets.fanout/server" })
    @ScriptProperty("networkAccept \"nukleus://streams/target#0\"")
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
            e.printStackTrace();
        }
    }
}
