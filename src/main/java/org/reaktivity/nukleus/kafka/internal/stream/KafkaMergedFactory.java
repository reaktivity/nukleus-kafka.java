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

import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.budget.MergedBudgetCreditor;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFetchDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaMergedBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaMetaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaResetExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaMergedFactory implements StreamFactory
{
    private static final int ERROR_NOT_LEADER_FOR_PARTITION = 6;

    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private final RouteFW routeRO = new RouteFW();
    private final KafkaRouteExFW routeExRO = new KafkaRouteExFW();

    private final BeginFW beginRO = new BeginFW();
    private final DataFW dataRO = new DataFW();
    private final EndFW endRO = new EndFW();
    private final AbortFW abortRO = new AbortFW();
    private final ResetFW resetRO = new ResetFW();
    private final WindowFW windowRO = new WindowFW();
    private final ExtensionFW extensionRO = new ExtensionFW();
    private final KafkaBeginExFW kafkaBeginExRO = new KafkaBeginExFW();
    private final KafkaDataExFW kafkaDataExRO = new KafkaDataExFW();
    private final KafkaResetExFW kafkaResetExRO = new KafkaResetExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaDataExFW.Builder kafkaDataExRW = new KafkaDataExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final int kafkaTypeId;
    private final RouteManager router;
    private final MutableDirectBuffer writeBuffer;
    private final MutableDirectBuffer extBuffer;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final Long2ObjectHashMap<MessageConsumer> correlations;
    private final MergedBudgetCreditor creditor;

    public KafkaMergedFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        LongSupplier supplyTraceId,
        ToIntFunction<String> supplyTypeId,
        Long2ObjectHashMap<MessageConsumer> correlations,
        MergedBudgetCreditor creditor)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.router = router;
        this.writeBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.extBuffer = new UnsafeBuffer(new byte[writeBuffer.capacity()]);
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.correlations = correlations;
        this.creditor = creditor;
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
        assert kafkaBeginEx.kind() == KafkaBeginExFW.KIND_MERGED;
        final KafkaMergedBeginExFW kafkaMergedBeginEx = kafkaBeginEx.merged();
        final String16FW beginTopic = kafkaMergedBeginEx.topic();
        final String topic = beginTopic != null ? beginTopic.asString() : null;

        final MessagePredicate filter = (t, b, i, l) ->
        {
            final RouteFW route = wrapRoute.apply(t, b, i, l);
            final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
            final String16FW routeTopic = routeEx != null ? routeEx.topic() : null;
            return route.localAddress().equals(route.remoteAddress()) &&
                    routeTopic != null && Objects.equals(routeTopic, beginTopic);
        };

        MessageConsumer newStream = null;

        final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
        if (route != null)
        {
            final long resolvedId = route.correlationId();
            final ArrayFW<KafkaOffsetFW> partitions = kafkaMergedBeginEx.partitions();

            final KafkaOffsetFW partition = partitions.matchFirst(p -> p.partitionId() == -1L);
            final long defaultOffset = partition != null ? partition.offset$() : -2; // EARLIEST ?

            final Long2LongHashMap initialOffsetsById = new Long2LongHashMap(-1L);
            partitions.forEach(p ->
            {
                final long partitionId = p.partitionId();
                if (partitionId >= 0L)
                {
                    final long partitionOffset = p.offset$();
                    initialOffsetsById.put(partitionId, partitionOffset);
                }
            });

            newStream = new KafkaMergedFetchStream(
                    sender,
                    routeId,
                    initialId,
                    affinity,
                    authorization,
                    topic,
                    resolvedId,
                    initialOffsetsById,
                    defaultOffset)::onMergedInitial;
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

    private void doData(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long budgetId,
        int reserved,
        int flags,
        OctetsFW payload,
        Flyweight extension)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .flags(flags)
                .budgetId(budgetId)
                .reserved(reserved)
                .payload(payload)
                .extension(extension.buffer(), extension.offset(), extension.sizeof())
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
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

    private final class KafkaMergedFetchStream
    {
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long authorization;
        private final String topic;
        private final long resolvedId;
        private final KafkaUnmergedMetaStream metaStream;
        private final List<KafkaUnmergedFetchStream> fetchStreams;
        private final Long2LongHashMap nextOffsetsById;
        private final long defaultOffset;

        private int state;

        private long replyBudgetId;
        private int replyBudget;
        private int replyPadding;
        private int fetchStreamIndex;
        private long mergedReplyBudgetId = NO_CREDITOR_INDEX;

        KafkaMergedFetchStream(
            MessageConsumer sender,
            long routeId,
            long initialId,
            long affinity,
            long authorization,
            String topic,
            long resolvedId,
            Long2LongHashMap initialOffsetsById,
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
            this.metaStream = new KafkaUnmergedMetaStream(this);
            this.fetchStreams = new ArrayList<>();
            this.nextOffsetsById = initialOffsetsById;
            this.defaultOffset = defaultOffset;
        }

        private void onMergedInitial(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onMergedInitialBegin(begin);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onMergedInitialEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onMergedInitialAbort(abort);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onMergedReplyWindow(window);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onMergedReplyReset(reset);
                break;
            default:
                break;
            }
        }

        private void onMergedInitialBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();

            assert state == 0;
            state = KafkaState.openingInitial(state);

            metaStream.doMetaInitialBegin(traceId);
        }

        private void onMergedInitialEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            metaStream.doMetaInitialEndIfNecessary(traceId);
            fetchStreams.forEach(f -> f.doFetchInitialEndIfNecessary(traceId));

            doMergedReplyEndIfNecessary(traceId);
        }

        private void onMergedInitialAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            metaStream.doMetaInitialAbortIfNecessary(traceId);
            fetchStreams.forEach(f -> f.doFetchInitialAbortIfNecessary(traceId));

            doMergedReplyAbortIfNecessary(traceId);
        }

        private void onMergedReplyWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            replyBudgetId = budgetId;
            replyBudget += credit;
            replyPadding = padding;

            state = KafkaState.openedReply(state);

            if (mergedReplyBudgetId == NO_CREDITOR_INDEX)
            {
                mergedReplyBudgetId = creditor.acquire(replyId, budgetId);
            }
            creditor.credit(traceId, mergedReplyBudgetId, credit);

            final int fetchStreamCount = fetchStreams.size();
            if (fetchStreamIndex >= fetchStreamCount)
            {
                fetchStreamIndex = 0;
            }

            for (int index = fetchStreamIndex; index < fetchStreamCount; index++)
            {
                final KafkaUnmergedFetchStream fetchStream = fetchStreams.get(index);
                fetchStream.doFetchReplyWindowIfNecessary(traceId);
            }

            for (int index = 0; index < fetchStreamIndex; index++)
            {
                final KafkaUnmergedFetchStream fetchStream = fetchStreams.get(index);
                fetchStream.doFetchReplyWindowIfNecessary(traceId);
            }

            fetchStreamIndex++;
        }

        private void onMergedReplyReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedReply(state);

            metaStream.doMetaReplyReset(traceId);
            fetchStreams.forEach(f -> f.doFetchReplyReset(traceId));

            doMergedInitialResetIfNecessary(traceId);
        }

        private void doMergedReplyBeginIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyOpening(state))
            {
                doMergedReplyBegin(traceId);
            }
        }

        private void doMergedReplyBegin(
            long traceId)
        {
            assert !KafkaState.replyOpening(state);
            state = KafkaState.openingReply(state);

            router.setThrottle(replyId, this::onMergedInitial);
            doBegin(sender, routeId, replyId, traceId, authorization, affinity, EMPTY_EXTENSION);
        }

        private void doMergedReplyData(
            long traceId,
            int flags,
            int reserved,
            OctetsFW payload,
            KafkaDataExFW kafkaDataEx)
        {
            replyBudget -= reserved;

            assert replyBudget >= 0;

            final KafkaFetchDataExFW kafkaFetchDataEx = kafkaDataEx.fetch();
            final KafkaOffsetFW partition = kafkaFetchDataEx.partition();
            final long timestamp = kafkaFetchDataEx.timestamp();
            final KafkaKeyFW key = kafkaFetchDataEx.key();
            final ArrayFW<KafkaHeaderFW> headers = kafkaFetchDataEx.headers();

            nextOffsetsById.put(partition.partitionId(), partition.offset$() + 1);

            final KafkaDataExFW newKafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                 .typeId(kafkaTypeId)
                 .merged(f -> f.timestamp(timestamp)
                               .partition(p -> p.partitionId(partition.partitionId())
                                                .offset$(partition.offset$()))
                               .progress(ps -> nextOffsetsById.longForEach((p, o) -> ps.item(i -> i.partitionId((int) p)
                                                                                                   .offset$(o))))
                               .key(k -> k.length(key.length())
                                          .value(key.value()))
                               .headers(hs -> headers.forEach(h -> hs.item(i -> i.nameLen(h.nameLen())
                                                                                 .name(h.name())
                                                                                 .valueLen(h.valueLen())
                                                                                 .value(h.value())))))
                 .build();

            doData(sender, routeId, replyId, traceId, authorization, replyBudgetId, reserved,
                flags, payload, newKafkaDataEx);
        }

        private void doMergedReplyEnd(
            long traceId)
        {
            assert !KafkaState.replyClosed(state);
            state = KafkaState.closedReply(state);
            doEnd(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doMergedReplyAbort(
            long traceId)
        {
            assert !KafkaState.replyClosed(state);
            state = KafkaState.closedReply(state);
            doAbort(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doMergedInitialWindowIfNecessary(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            if (!KafkaState.initialOpened(state) || credit > 0)
            {
                doMergedInitialWindow(traceId, budgetId, credit, padding);
            }
        }

        private void doMergedInitialWindow(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            state = KafkaState.openedInitial(state);

            doWindow(sender, routeId, initialId, traceId, authorization,
                    budgetId, credit, padding);
        }

        private void doMergedInitialReset(
            long traceId)
        {
            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            doReset(sender, routeId, initialId, traceId, authorization);
        }

        private void doMergedReplyEndIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doMergedReplyEnd(traceId);
            }
        }

        private void doMergedReplyAbortIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosed(state))
            {
                doMergedReplyAbort(traceId);
            }
        }

        private void doMergedInitialResetIfNecessary(
            long traceId)
        {
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
            {
                doMergedInitialReset(traceId);
            }
        }

        private void doMergedCleanup(
            long traceId)
        {
            if (mergedReplyBudgetId != NO_CREDITOR_INDEX)
            {
                creditor.release(mergedReplyBudgetId);
                mergedReplyBudgetId = NO_CREDITOR_INDEX;
            }

            doMergedInitialResetIfNecessary(traceId);
            doMergedReplyAbortIfNecessary(traceId);

            metaStream.doMetaCleanup(traceId);
            fetchStreams.forEach(f -> f.doFetchCleanup(traceId));
        }

        private void onTopicMetaDataChanged(
            long traceId,
            ArrayFW<KafkaPartitionFW> partitions)
        {
            partitions.forEach(partition -> onPartitionMetaDataChangedIfNecessary(traceId, partition));
        }

        private void onPartitionMetaDataChangedIfNecessary(
            long traceId,
            KafkaPartitionFW partition)
        {
            final int partitionId = partition.partitionId();
            final int leaderId = partition.leaderId();

            KafkaUnmergedFetchStream oldLeader = null;
            for (int index = 0; index < fetchStreams.size(); index++)
            {
                final KafkaUnmergedFetchStream fetchStream = fetchStreams.get(index);
                if (fetchStream.partitionId == partitionId && fetchStream.leaderId == leaderId)
                {
                    oldLeader = fetchStream;
                    break;
                }
            }
            assert oldLeader == null || oldLeader.partitionId == partitionId;

            if (oldLeader != null && oldLeader.leaderId != leaderId)
            {
                oldLeader.doFetchInitialEndIfNecessary(traceId);
                //oldLeader.doFetchReplyResetIfNecessary(traceId);
            }

            if (oldLeader == null || oldLeader.leaderId != leaderId)
            {
                long partitionOffset = nextOffsetsById.get(partitionId);
                if (partitionOffset == nextOffsetsById.missingValue())
                {
                    partitionOffset = defaultOffset;
                }

                final KafkaUnmergedFetchStream newLeader = new KafkaUnmergedFetchStream(partitionId, leaderId, this);
                newLeader.doFetchInitialBegin(traceId, partitionOffset);

                fetchStreams.add(newLeader);
            }
        }

        private void onPartitionReady(
            long traceId,
            long partitionId)
        {
            nextOffsetsById.putIfAbsent(partitionId, defaultOffset);

            if (nextOffsetsById.size() == fetchStreams.size())
            {
                doMergedReplyBeginIfNecessary(traceId);

                if (KafkaState.initialClosed(state))
                {
                    doMergedReplyEndIfNecessary(traceId);
                }
            }
        }
    }

    private final class KafkaUnmergedMetaStream
    {
        private final KafkaMergedFetchStream mergedFetch;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private int replyBudget;

        private KafkaUnmergedMetaStream(
            KafkaMergedFetchStream mergedFetch)
        {
            this.mergedFetch = mergedFetch;
        }

        private void doMetaInitialBegin(
            long traceId)
        {
            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(mergedFetch.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onMetaReply);
            router.setThrottle(initialId, this::onMetaReply);
            doBegin(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, 0L,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .meta(m -> m.topic(mergedFetch.topic))
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

            doEnd(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, EMPTY_EXTENSION);
        }

        private void doMetaInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doMetaInitialAbort(traceId);
            }
        }

        private void doMetaInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, EMPTY_EXTENSION);
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
                mergedFetch.doMergedCleanup(traceId);
            }
            else
            {
                final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::wrap);
                final KafkaMetaDataExFW kafkaMetaDataEx = kafkaDataEx.meta();
                final ArrayFW<KafkaPartitionFW> partitions = kafkaMetaDataEx.partitions();
                mergedFetch.onTopicMetaDataChanged(traceId, partitions);

                doMetaReplyWindow(traceId, reserved);
            }
        }

        private void onMetaReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            mergedFetch.doMergedReplyBeginIfNecessary(traceId);
            mergedFetch.doMergedReplyEndIfNecessary(traceId);

            doMetaInitialEndIfNecessary(traceId);
        }

        private void onMetaReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            mergedFetch.doMergedReplyAbortIfNecessary(traceId);

            doMetaInitialAbortIfNecessary(traceId);
        }

        private void onMetaInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedInitial(state);

            mergedFetch.doMergedInitialResetIfNecessary(traceId);

            doMetaReplyResetIfNecessary(traceId);
        }

        private void onMetaInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                mergedFetch.doMergedInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }
        }

        private void doMetaReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            replyBudget += credit;

            doWindow(receiver, mergedFetch.resolvedId, replyId, traceId, mergedFetch.authorization,
                0L, credit, mergedFetch.replyPadding);
        }

        private void doMetaReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doMetaReplyReset(traceId);
            }
        }

        private void doMetaReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);

            doReset(receiver, mergedFetch.resolvedId, replyId, traceId, mergedFetch.authorization);
        }

        private void doMetaCleanup(
            long traceId)
        {
            doMetaInitialAbortIfNecessary(traceId);
            doMetaReplyResetIfNecessary(traceId);
        }
    }

    private final class KafkaUnmergedFetchStream
    {
        private final int leaderId;
        private final int partitionId;
        private final KafkaMergedFetchStream mergedFetch;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver;

        private int state;

        private int replyBudget;

        private KafkaUnmergedFetchStream(
            int partitionId,
            int leaderId,
            KafkaMergedFetchStream mergedFetch)
        {
            this.leaderId = leaderId;
            this.partitionId = partitionId;
            this.mergedFetch = mergedFetch;
        }

        private void doFetchInitialBegin(
            long traceId,
            long partitionOffset)
        {
            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(mergedFetch.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onFetchReply);
            router.setThrottle(initialId, this::onFetchReply);
            doBegin(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.topic(mergedFetch.topic)
                                     .partition(p -> p.partitionId(partitionId).offset$(partitionOffset)))
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

            doEnd(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, EMPTY_EXTENSION);
        }

        private void doFetchInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doFetchInitialAbort(traceId);
            }
        }

        private void doFetchInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, EMPTY_EXTENSION);
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
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onFetchReplyData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onFetchReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onFetchReplyAbort(abort);
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

            mergedFetch.onPartitionReady(traceId, partitionId);

            doFetchReplyWindowIfNecessary(traceId);
        }

        private void onFetchReplyData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();

            assert budgetId == mergedFetch.mergedReplyBudgetId;

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                mergedFetch.doMergedCleanup(traceId);
            }
            else
            {
                final int flags = data.flags();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();
                final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::wrap);

                mergedFetch.doMergedReplyData(traceId, flags, reserved, payload, kafkaDataEx);
            }
        }

        private void onFetchReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            mergedFetch.doMergedReplyEndIfNecessary(traceId);

            doFetchInitialEndIfNecessary(traceId);
        }

        private void onFetchReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            mergedFetch.doMergedReplyAbortIfNecessary(traceId);

            doFetchInitialAbortIfNecessary(traceId);
        }

        private void onFetchInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final OctetsFW extension = reset.extension();

            state = KafkaState.closedInitial(state);

            final KafkaResetExFW kafkaResetEx = extension.get(kafkaResetExRO::wrap);
            final int error = kafkaResetEx != null ? kafkaResetEx.error() : 0;

            doFetchReplyResetIfNecessary(traceId);

            if (error != ERROR_NOT_LEADER_FOR_PARTITION)
            {
                mergedFetch.doMergedInitialResetIfNecessary(traceId);
            }
        }

        private void onFetchInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                mergedFetch.doMergedInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }
        }

        private void doFetchReplyWindowIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpening(state) && !KafkaState.replyClosing(state))
            {
                state = KafkaState.openedReply(state);

                final int credit = mergedFetch.replyBudget - replyBudget;
                if (credit > 0)
                {
                    replyBudget += credit;

                    doWindow(receiver, mergedFetch.resolvedId, replyId, traceId, mergedFetch.authorization,
                        mergedFetch.mergedReplyBudgetId, credit, mergedFetch.replyPadding);
                }
            }
        }

        private void doFetchReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doFetchReplyReset(traceId);
            }
        }

        private void doFetchReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);

            doReset(receiver, mergedFetch.resolvedId, replyId, traceId, mergedFetch.authorization);
        }

        private void doFetchCleanup(
            long traceId)
        {
            doFetchInitialAbortIfNecessary(traceId);
            doFetchReplyResetIfNecessary(traceId);
        }
    }
}
