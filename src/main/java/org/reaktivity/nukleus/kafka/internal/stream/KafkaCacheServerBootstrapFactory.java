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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaConfigFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaPartitionFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.String16FW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ExtensionFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBootstrapBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDescribeDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFetchFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaMetaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaResetExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaCacheServerBootstrapFactory implements StreamFactory
{
    private static final String16FW CONFIG_NAME_CLEANUP_POLICY = new String16FW("cleanup.policy");
    private static final String16FW CONFIG_NAME_MAX_MESSAGE_BYTES = new String16FW("max.message.bytes");
    private static final String16FW CONFIG_NAME_SEGMENT_BYTES = new String16FW("segment.bytes");
    private static final String16FW CONFIG_NAME_SEGMENT_INDEX_BYTES = new String16FW("segment.index.bytes");
    private static final String16FW CONFIG_NAME_SEGMENT_MILLIS = new String16FW("segment.ms");
    private static final String16FW CONFIG_NAME_RETENTION_BYTES = new String16FW("retention.bytes");
    private static final String16FW CONFIG_NAME_RETENTION_MILLIS = new String16FW("retention.ms");
    private static final String16FW CONFIG_NAME_DELETE_RETENTION_MILLIS = new String16FW("delete.retention.ms");
    private static final String16FW CONFIG_NAME_MIN_COMPACTION_LAG_MILLIS = new String16FW("min.compaction.lag.ms");
    private static final String16FW CONFIG_NAME_MAX_COMPACTION_LAG_MILLIS = new String16FW("max.compaction.lag.ms");
    private static final String16FW CONFIG_NAME_MIN_CLEANABLE_DIRTY_RATIO = new String16FW("min.cleanable.dirty.ratio");

    private static final long OFFSET_EARLIEST = KafkaOffsetType.EARLIEST.value();

    private static final int ERROR_NOT_LEADER_FOR_PARTITION = 6;

    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final FlushFW flushRO = new FlushFW();
    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();
    private final KafkaDataExFW kafkaDataExRO = new KafkaDataExFW();
    private final KafkaFlushExFW kafkaFlushExRO = new KafkaFlushExFW();
    private final KafkaResetExFW kafkaResetExRO = new KafkaResetExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final MutableInteger partitionCount = new MutableInteger();

    private final int kafkaTypeId;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final Long2ObjectHashMap<MessageConsumer> correlations;

    public KafkaCacheServerBootstrapFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        Long2ObjectHashMap<MessageConsumer> correlations)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.router = router;
        this.writeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.correlations = correlations;
    }

    @Override
    public MessageConsumer newStream(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length,
        MessageConsumer sender)
    {
        final BeginFW begin = beginRO.wrap(buffer, index, index + length);
        final long routeId = begin.routeId();
        final long initialId = begin.streamId();
        final long authorization = begin.authorization();
        final long affinity = begin.affinity();

        assert (initialId & 0x0000_0000_0000_0001L) != 0L;

        final OctetsFW extension = begin.extension();
        final ExtensionFW beginEx = extensionRO.tryWrap(extension.buffer(), extension.offset(), extension.limit());
        final KafkaBeginExFW kafkaBeginEx = beginEx != null && beginEx.typeId() == kafkaTypeId ?
                kafkaBeginExRO.tryWrap(extension.buffer(), extension.offset(), extension.limit()) : null;

        assert kafkaBeginEx != null;
        assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_BOOTSTRAP;
        final KafkaBootstrapBeginExFW kafkaBootstrapBeginEx = kafkaBeginEx.bootstrap();
        final String16FW beginTopic = kafkaBootstrapBeginEx.topic();
        final String topic = beginTopic != null ? beginTopic.asString() : null;

        final MessagePredicate filter = (t, b, i, l) ->
        {
            final RouteFW route = wrapRoute.apply(t, b, i, l);
            final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
            final String16FW routeTopic = routeEx != null ? routeEx.topic() : null;
            return route.localAddress().equals(route.remoteAddress()) &&
                    (beginTopic != null && (routeTopic == null || routeTopic.equals(beginTopic)));
        };

        MessageConsumer newStream = null;

        final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
        if (route != null)
        {
            final long resolvedId = route.correlationId();
            final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
            final long defaultOffset = routeEx != null ? routeEx.defaultOffset().get().value() : OFFSET_EARLIEST;

            newStream = new KafkaBootstrapStream(
                    sender,
                    routeId,
                    initialId,
                    affinity,
                    authorization,
                    topic,
                    resolvedId,
                    defaultOffset)::onBootstrapInitial;
        }

        return newStream;
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long affinity,
        Consumer<OctetsFW.Builder> extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(extension)
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    private void doEnd(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Consumer<OctetsFW.Builder> extension)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                               .routeId(routeId)
                               .streamId(streamId)
                               .traceId(traceId)
                               .authorization(authorization)
                               .extension(extension)
                               .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private void doAbort(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        Consumer<OctetsFW.Builder> extension)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .extension(extension)
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    private void doWindow(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int credit,
        int padding)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .credit(credit)
                .padding(padding)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    private void doReset(
        MessageConsumer sender,
        long routeId,
        long streamId,
        long traceId,
        long authorization)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
               .routeId(routeId)
               .streamId(streamId)
               .traceId(traceId)
               .authorization(authorization)
               .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    private final class KafkaBootstrapStream
    {
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long authorization;
        private final String topic;
        private final long resolvedId;
        private final KafkaBootstrapDescribeStream describeStream;
        private final KafkaBootstrapMetaStream metaStream;
        private final List<KafkaBootstrapFetchStream> fetchStreams;
        private final Long2LongHashMap nextOffsetsById;
        private final long defaultOffset;

        private int state;

        private long replyBudgetId;
        private int replyPadding;

        KafkaBootstrapStream(
            MessageConsumer sender,
            long routeId,
            long initialId,
            long affinity,
            long authorization,
            String topic,
            long resolvedId,
            long defaultOffset)
        {
            this.sender = sender;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.affinity = affinity;
            this.authorization = authorization;
            this.topic = topic;
            this.resolvedId = resolvedId;
            this.describeStream = new KafkaBootstrapDescribeStream(this);
            this.metaStream = new KafkaBootstrapMetaStream(this);
            this.fetchStreams = new ArrayList<>();
            this.nextOffsetsById = new Long2LongHashMap(-1L);
            this.defaultOffset = defaultOffset;
        }

        private void onBootstrapInitial(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBootstrapInitialBegin(begin);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onBootstrapInitialEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onBootstrapInitialAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onBootstrapReplyWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onBootstrapReplyReset(reset);
                break;
            default:
                break;
            }
        }

        private void onBootstrapInitialBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();

            assert state == 0;
            state = KafkaState.openingInitial(state);

            describeStream.doDescribeInitialBegin(traceId);
        }

        private void onBootstrapInitialEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            describeStream.doDescribeInitialEndIfNecessary(traceId);
            metaStream.doMetaInitialEndIfNecessary(traceId);
            fetchStreams.forEach(f -> f.doFetchInitialEndIfNecessary(traceId));

            doBootstrapReplyEndIfNecessary(traceId);
        }

        private void onBootstrapInitialAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            describeStream.doDescribeInitialAbortIfNecessary(traceId);
            metaStream.doMetaInitialAbortIfNecessary(traceId);
            fetchStreams.forEach(f -> f.doFetchInitialAbortIfNecessary(traceId));

            doBootstrapReplyAbortIfNecessary(traceId);
        }

        private void onBootstrapReplyWindow(
            WindowFW window)
        {
            final long budgetId = window.budgetId();
            final int padding = window.padding();

            replyBudgetId = budgetId;
            replyPadding = padding;

            state = KafkaState.openedReply(state);
        }

        private void onBootstrapReplyReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedReply(state);

            describeStream.doDescribeReplyReset(traceId);
            metaStream.doMetaReplyReset(traceId);
            fetchStreams.forEach(f -> f.doFetchReplyReset(traceId));

            doBootstrapInitialResetIfNecessary(traceId);
        }

        private void doBootstrapReplyBeginIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyOpening(state))
            {
                doBootstrapReplyBegin(traceId);
            }
        }

        private void doBootstrapReplyBegin(
            long traceId)
        {
            assert !KafkaState.replyOpening(state);
            state = KafkaState.openingReply(state);

            router.setThrottle(replyId, this::onBootstrapInitial);
            doBegin(sender, routeId, replyId, traceId, authorization, affinity, EMPTY_EXTENSION);
        }

        private void doBootstrapReplyEnd(
            long traceId)
        {
            assert !KafkaState.replyClosed(state);
            state = KafkaState.closedReply(state);
            doEnd(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doBootstrapReplyAbort(
            long traceId)
        {
            assert !KafkaState.replyClosed(state);
            state = KafkaState.closedReply(state);
            doAbort(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doBootstrapInitialWindowIfNecessary(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doBootstrapInitialWindow(traceId, budgetId, credit, padding);
            }
        }

        private void doBootstrapInitialWindow(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            state = KafkaState.openedInitial(state);

            doWindow(sender, routeId, initialId, traceId, authorization,
                    budgetId, credit, padding);
        }

        private void doBootstrapInitialReset(
            long traceId)
        {
            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            doReset(sender, routeId, initialId, traceId, authorization);
        }

        private void doBootstrapReplyEndIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doBootstrapReplyEnd(traceId);
            }
        }

        private void doBootstrapReplyAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doBootstrapReplyAbort(traceId);
            }
        }

        private void doBootstrapInitialResetIfNecessary(
            long traceId)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doBootstrapInitialReset(traceId);
            }
        }

        private void doBootstrapCleanup(
            long traceId)
        {
            doBootstrapInitialResetIfNecessary(traceId);
            doBootstrapReplyAbortIfNecessary(traceId);

            describeStream.doBootstrapDescribeCleanup(traceId);
            metaStream.doMetaCleanup(traceId);
            fetchStreams.forEach(f -> f.doBootstrapFetchCleanup(traceId));
        }

        private void onTopicConfigChanged(
            long traceId,
            ArrayFW<KafkaConfigFW> changedConfigs)
        {
            metaStream.doMetaInitialBeginIfNecessary(traceId);
        }

        private void onTopicMetaDataChanged(
            long traceId,
            ArrayFW<KafkaPartitionFW> partitions)
        {
            partitions.forEach(partition -> onPartitionMetaDataChangedIfNecessary(traceId, partition));

            partitionCount.value = 0;
            partitions.forEach(partition -> partitionCount.value++);
            assert fetchStreams.size() >= partitionCount.value;
        }

        private void onPartitionMetaDataChangedIfNecessary(
            long traceId,
            KafkaPartitionFW partition)
        {
            final int partitionId = partition.partitionId();
            final int leaderId = partition.leaderId();
            final long partitionOffset = nextPartitionOffset(partitionId);

            KafkaBootstrapFetchStream leader = findPartitionLeader(partitionId);

            if (leader != null && leader.leaderId != leaderId)
            {
                leader.leaderId = leaderId;
                leader.doFetchInitialBeginIfNecessary(traceId, partitionOffset);
            }

            if (leader == null)
            {
                leader = new KafkaBootstrapFetchStream(partitionId, leaderId, this);
                leader.doFetchInitialBegin(traceId, partitionOffset);
                fetchStreams.add(leader);
            }

            assert leader != null;
            assert leader.partitionId == partitionId;
            assert leader.leaderId == leaderId;
        }

        private void onPartitionLeaderReady(
            long traceId,
            long partitionId)
        {
            nextOffsetsById.putIfAbsent(partitionId, defaultOffset);

            if (nextOffsetsById.size() == fetchStreams.size())
            {
                doBootstrapReplyBeginIfNecessary(traceId);

                if (KafkaState.initialClosed(state))
                {
                    doBootstrapReplyEndIfNecessary(traceId);
                }
            }
        }

        private void onPartitionLeaderError(
            long traceId,
            int partitionId,
            int error)
        {
            if (error == ERROR_NOT_LEADER_FOR_PARTITION)
            {
                final KafkaBootstrapFetchStream leader = findPartitionLeader(partitionId);
                assert leader != null;

                if (nextOffsetsById.containsKey(partitionId))
                {
                    final long partitionOffset = nextPartitionOffset(partitionId);
                    leader.doFetchInitialBegin(traceId, partitionOffset);
                }
                else
                {
                    fetchStreams.remove(leader);
                }
            }
            else
            {
                doBootstrapCleanup(traceId);
            }
        }

        private long nextPartitionOffset(
            int partitionId)
        {
            long partitionOffset = nextOffsetsById.get(partitionId);
            if (partitionOffset == nextOffsetsById.missingValue())
            {
                partitionOffset = defaultOffset;
            }
            return partitionOffset;
        }

        private KafkaBootstrapFetchStream findPartitionLeader(
            int partitionId)
        {
            KafkaBootstrapFetchStream leader = null;
            for (int index = 0; index < fetchStreams.size(); index++)
            {
                final KafkaBootstrapFetchStream fetchStream = fetchStreams.get(index);
                if (fetchStream.partitionId == partitionId)
                {
                    leader = fetchStream;
                    break;
                }
            }
            return leader;
        }
    }

    private final class KafkaBootstrapDescribeStream
    {
        private final KafkaBootstrapStream bootstrap;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private int replyBudget;

        private KafkaBootstrapDescribeStream(
            KafkaBootstrapStream bootstrap)
        {
            this.bootstrap = bootstrap;
        }

        private void doDescribeInitialBegin(
            long traceId)
        {
            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(bootstrap.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onDescribeReply);
            router.setThrottle(initialId, this::onDescribeReply);
            doBegin(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, 0L,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .describe(m -> m.topic(bootstrap.topic)
                                        .configsItem(ci -> ci.set(CONFIG_NAME_CLEANUP_POLICY))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_MAX_MESSAGE_BYTES))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_SEGMENT_BYTES))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_SEGMENT_INDEX_BYTES))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_SEGMENT_MILLIS))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_RETENTION_BYTES))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_RETENTION_MILLIS))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_DELETE_RETENTION_MILLIS))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_MIN_COMPACTION_LAG_MILLIS))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_MAX_COMPACTION_LAG_MILLIS))
                                        .configsItem(ci -> ci.set(CONFIG_NAME_MIN_CLEANABLE_DIRTY_RATIO)))
                        .build()
                        .sizeof()));
        }

        private void doDescribeInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doDescribeInitialEnd(traceId);
            }
        }

        private void doDescribeInitialEnd(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doEnd(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void doDescribeInitialAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doDescribeInitialAbort(traceId);
            }
        }

        private void doDescribeInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void onDescribeReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onDescribeReplyBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onDescribeReplyData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onDescribeReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onDescribeReplyAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onDescribeInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onDescribeInitialWindow(window);
                break;
            default:
                break;
            }
        }

        private void onDescribeReplyBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();

            state = KafkaState.openedReply(state);

            doDescribeReplyWindow(traceId, 8192);
        }

        private void onDescribeReplyData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int reserved = data.reserved();
            final OctetsFW extension = data.extension();

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                bootstrap.doBootstrapCleanup(traceId);
            }
            else
            {
                final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::wrap);
                final KafkaDescribeDataExFW kafkaDescribeDataEx = kafkaDataEx.describe();
                final ArrayFW<KafkaConfigFW> changedConfigs = kafkaDescribeDataEx.configs();

                bootstrap.onTopicConfigChanged(traceId, changedConfigs);

                doDescribeReplyWindow(traceId, reserved);
            }
        }

        private void onDescribeReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyBeginIfNecessary(traceId);
            bootstrap.doBootstrapReplyEndIfNecessary(traceId);

            doDescribeInitialEndIfNecessary(traceId);
        }

        private void onDescribeReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyAbortIfNecessary(traceId);

            doDescribeInitialAbortIfNecessary(traceId);
        }

        private void onDescribeInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedInitial(state);

            doDescribeReplyResetIfNecessary(traceId);

            bootstrap.doBootstrapCleanup(traceId);
        }

        private void onDescribeInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                bootstrap.doBootstrapInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }
        }

        private void doDescribeReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            replyBudget += credit;

            doWindow(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization,
                0L, credit, bootstrap.replyPadding);
        }

        private void doDescribeReplyResetIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doDescribeReplyReset(traceId);
            }
        }

        private void doDescribeReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);

            doReset(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization);
        }

        private void doBootstrapDescribeCleanup(
            long traceId)
        {
            doDescribeInitialAbortIfNecessary(traceId);
            doDescribeReplyResetIfNecessary(traceId);
        }
    }

    private final class KafkaBootstrapMetaStream
    {
        private final KafkaBootstrapStream bootstrap;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private int replyBudget;

        private KafkaBootstrapMetaStream(
            KafkaBootstrapStream mergedFetch)
        {
            this.bootstrap = mergedFetch;
        }

        private void doMetaInitialBeginIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialOpening(state))
            {
                doMetaInitialBegin(traceId);
            }
        }

        private void doMetaInitialBegin(
            long traceId)
        {
            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(bootstrap.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onMetaReply);
            router.setThrottle(initialId, this::onMetaReply);
            doBegin(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, 0L,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .meta(m -> m.topic(bootstrap.topic))
                        .build()
                        .sizeof()));
        }

        private void doMetaInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doMetaInitialEnd(traceId);
            }
        }

        private void doMetaInitialEnd(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doEnd(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void doMetaInitialAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doMetaInitialAbort(traceId);
            }
        }

        private void doMetaInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void onMetaReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onMetaReplyBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onMetaReplyData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onMetaReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onMetaReplyAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onMetaInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onMetaInitialWindow(window);
                break;
            default:
                break;
            }
        }

        private void onMetaReplyBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();

            state = KafkaState.openedReply(state);

            doMetaReplyWindow(traceId, 8192);
        }

        private void onMetaReplyData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final int reserved = data.reserved();
            final OctetsFW extension = data.extension();

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                bootstrap.doBootstrapCleanup(traceId);
            }
            else
            {
                final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::wrap);
                final KafkaMetaDataExFW kafkaMetaDataEx = kafkaDataEx.meta();
                final ArrayFW<KafkaPartitionFW> partitions = kafkaMetaDataEx.partitions();

                bootstrap.onTopicMetaDataChanged(traceId, partitions);

                doMetaReplyWindow(traceId, reserved);
            }
        }

        private void onMetaReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyBeginIfNecessary(traceId);
            bootstrap.doBootstrapReplyEndIfNecessary(traceId);

            doMetaInitialEndIfNecessary(traceId);
        }

        private void onMetaReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyAbortIfNecessary(traceId);

            doMetaInitialAbortIfNecessary(traceId);
        }

        private void onMetaInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedInitial(state);

            doMetaReplyResetIfNecessary(traceId);

            bootstrap.doBootstrapCleanup(traceId);
        }

        private void onMetaInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                bootstrap.doBootstrapInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }
        }

        private void doMetaReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            replyBudget += credit;

            doWindow(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization,
                0L, credit, bootstrap.replyPadding);
        }

        private void doMetaReplyResetIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doMetaReplyReset(traceId);
            }
        }

        private void doMetaReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);

            doReset(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization);
        }

        private void doMetaCleanup(
            long traceId)
        {
            doMetaInitialAbortIfNecessary(traceId);
            doMetaReplyResetIfNecessary(traceId);
        }
    }

    private final class KafkaBootstrapFetchStream
    {
        private final int partitionId;
        private final KafkaBootstrapStream bootstrap;

        private int leaderId;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private int replyBudget;

        @SuppressWarnings("unused")
        private long partitionOffset;

        private KafkaBootstrapFetchStream(
            int partitionId,
            int leaderId,
            KafkaBootstrapStream bootstrap)
        {
            this.leaderId = leaderId;
            this.partitionId = partitionId;
            this.bootstrap = bootstrap;
        }

        private void doFetchInitialBeginIfNecessary(
            long traceId,
            long partitionOffset)
        {
            if (!KafkaState.initialOpening(state))
            {
                doFetchInitialBegin(traceId, partitionOffset);
            }
        }

        private void doFetchInitialBegin(
            long traceId,
            long partitionOffset)
        {
            if (KafkaState.closed(state))
            {
                state = 0;
            }

            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(bootstrap.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onFetchReply);
            router.setThrottle(initialId, this::onFetchReply);
            doBegin(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.topic(bootstrap.topic)
                                     .partition(p -> p.partitionId(partitionId).partitionOffset(OFFSET_EARLIEST))
                                     .deltaType(t -> t.set(KafkaDeltaType.NONE)))
                        .build()
                        .sizeof()));
        }

        private void doFetchInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doFetchInitialEnd(traceId);
            }
        }

        private void doFetchInitialEnd(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doEnd(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void doFetchInitialAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doFetchInitialAbort(traceId);
            }
        }

        private void doFetchInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void onFetchInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final OctetsFW extension = reset.extension();

            state = KafkaState.closedInitial(state);

            final KafkaResetExFW kafkaResetEx = extension.get(kafkaResetExRO::tryWrap);
            final int error = kafkaResetEx != null ? kafkaResetEx.error() : -1;

            doFetchReplyResetIfNecessary(traceId);

            bootstrap.onPartitionLeaderError(traceId, partitionId, error);
        }

        private void onFetchInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                bootstrap.doBootstrapInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }
        }

        private void onFetchReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onFetchReplyBegin(begin);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onFetchReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onFetchReplyAbort(abort);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onFetchReplyFlush(flush);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onFetchInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onFetchInitialWindow(window);
                break;
            default:
                break;
            }
        }

        private void onFetchReplyBegin(
            BeginFW begin)
        {
            state = KafkaState.openingReply(state);

            final long traceId = begin.traceId();

            bootstrap.onPartitionLeaderReady(traceId, partitionId);

            doFetchReplyWindow(traceId, 8192); // TODO: consider 0 to avoid receiving FLUSH frames
        }

        private void onFetchReplyFlush(
            FlushFW flush)
        {
            final long traceId = flush.traceId();
            final int reserved = flush.reserved();

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                bootstrap.doBootstrapCleanup(traceId);
            }
            else
            {
                final OctetsFW extension = flush.extension();
                final KafkaFlushExFW kafkaFlushEx = extension.get(kafkaFlushExRO::wrap);
                final KafkaFetchFlushExFW kafkaFetchFlushEx = kafkaFlushEx.fetch();
                final KafkaOffsetFW partition = kafkaFetchFlushEx.partition();

                this.partitionOffset = partition.partitionOffset();

                doFetchReplyWindow(traceId, reserved);
            }
        }

        private void onFetchReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyEndIfNecessary(traceId);

            doFetchInitialEndIfNecessary(traceId);
        }

        private void onFetchReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyAbortIfNecessary(traceId);

            doFetchInitialAbortIfNecessary(traceId);
        }

        private void doFetchReplyWindow(
            long traceId,
            int credit)
        {
            if (!KafkaState.replyOpened(state) || credit > 0)
            {
                state = KafkaState.openedReply(state);

                replyBudget += credit;

                doWindow(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization,
                    bootstrap.replyBudgetId, credit, 0);
            }
        }

        private void doFetchReplyResetIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doFetchReplyReset(traceId);
            }
        }

        private void doFetchReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);

            doReset(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization);
        }

        private void doBootstrapFetchCleanup(
            long traceId)
        {
            doFetchInitialAbortIfNecessary(traceId);
            doFetchReplyResetIfNecessary(traceId);
        }
    }
}
