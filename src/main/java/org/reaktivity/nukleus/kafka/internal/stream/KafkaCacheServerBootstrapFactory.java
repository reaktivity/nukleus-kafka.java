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
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.FlushFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBootstrapBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFetchFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaMetaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaCacheServerBootstrapFactory implements StreamFactory
{
    private static final long OFFSET_EARLIEST = -2L;

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

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

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
                    routeTopic != null && Objects.equals(routeTopic, beginTopic);
        };

        MessageConsumer newStream = null;

        final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
        if (route != null)
        {
            final long resolvedId = route.correlationId();
            final Long2LongHashMap initialOffsetsById = new Long2LongHashMap(-1L);
            final long defaultOffset = OFFSET_EARLIEST;

            newStream = new KafkaBootstrapStream(
                    sender,
                    routeId,
                    initialId,
                    affinity,
                    authorization,
                    topic,
                    resolvedId,
                    initialOffsetsById,
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
            this.metaStream = new KafkaBootstrapMetaStream(this);
            this.fetchStreams = new ArrayList<>();
            this.nextOffsetsById = initialOffsetsById;
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

            metaStream.doBootstrapMetaInitialBegin(traceId);
        }

        private void onBootstrapInitialEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            metaStream.doBootstrapMetaInitialEndIfNecessary(traceId);
            fetchStreams.forEach(f -> f.doBootstrapFetchInitialEndIfNecessary(traceId));

            doBootstrapReplyEndIfNecessary(traceId);
        }

        private void onBootstrapInitialAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            metaStream.doBootstrapMetaInitialAbortIfNecessary(traceId);
            fetchStreams.forEach(f -> f.doBootstrapFetchInitialAbortIfNecessary(traceId));

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

            metaStream.doBootstrapMetaReplyReset(traceId);
            fetchStreams.forEach(f -> f.doBootstrapFetchReplyReset(traceId));

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

            metaStream.doBootstrapMetaCleanup(traceId);
            fetchStreams.forEach(f -> f.doBootstrapFetchCleanup(traceId));
        }

        private void onBootstrapTopicMetaDataChanged(
            long traceId,
            ArrayFW<KafkaPartitionFW> partitions)
        {
            partitions.forEach(partition -> onBootstrapPartitionMetaDataChangedIfNecessary(traceId, partition));
        }

        private void onBootstrapPartitionMetaDataChangedIfNecessary(
            long traceId,
            KafkaPartitionFW partition)
        {
            final int partitionId = partition.partitionId();
            final int leaderId = partition.leaderId();

            KafkaBootstrapFetchStream oldLeader = null;
            for (int index = 0; index < fetchStreams.size(); index++)
            {
                final KafkaBootstrapFetchStream fetchStream = fetchStreams.get(index);
                if (fetchStream.partitionId == partitionId && fetchStream.leaderId == leaderId)
                {
                    oldLeader = fetchStream;
                    break;
                }
            }
            assert oldLeader == null || oldLeader.partitionId == partitionId;

            if (oldLeader != null && oldLeader.leaderId != leaderId)
            {
                oldLeader.doBootstrapFetchInitialEndIfNecessary(traceId);
                //oldLeader.doBootstrapFetchReplyResetIfNecessary(traceId);
            }

            if (oldLeader == null || oldLeader.leaderId != leaderId)
            {
                // TODO: require successful claim leaderId for this elektron index

                long partitionOffset = nextOffsetsById.get(partitionId);
                if (partitionOffset == nextOffsetsById.missingValue())
                {
                    partitionOffset = defaultOffset;
                }

                final KafkaBootstrapFetchStream newLeader = new KafkaBootstrapFetchStream(partitionId, leaderId, this);
                newLeader.doBootstrapInitialBegin(traceId, partitionOffset);

                fetchStreams.add(newLeader);
            }
        }

        private void onBootstrapPartitionReady(
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

        private void doBootstrapMetaInitialBegin(
            long traceId)
        {
            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(bootstrap.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onBootstrapMetaReply);
            router.setThrottle(initialId, this::onBootstrapMetaReply);
            doBegin(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, 0L,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .meta(m -> m.topic(bootstrap.topic))
                        .build()
                        .sizeof()));
        }

        private void doBootstrapMetaInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doBootstrapMetaInitialEnd(traceId);
            }
        }

        private void doBootstrapMetaInitialEnd(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doEnd(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void doBootstrapMetaInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doBootstrapMetaInitialAbort(traceId);
            }
        }

        private void doBootstrapMetaInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void onBootstrapMetaReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBootstrapMetaReplyBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onBootstrapMetaReplyData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onBootstrapMetaReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onBootstrapMetaReplyAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onBootstrapMetaInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onBootstrapMetaInitialWindow(window);
                break;
            default:
                break;
            }
        }

        private void onBootstrapMetaReplyBegin(
            BeginFW begin)
        {
            final long traceId = begin.traceId();

            state = KafkaState.openedReply(state);

            doBootstrapMetaReplyWindow(traceId, 8192);
        }

        private void onBootstrapMetaReplyData(
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

                bootstrap.onBootstrapTopicMetaDataChanged(traceId, partitions);

                doBootstrapMetaReplyWindow(traceId, reserved);
            }
        }

        private void onBootstrapMetaReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyBeginIfNecessary(traceId);
            bootstrap.doBootstrapReplyEndIfNecessary(traceId);

            doBootstrapMetaInitialEndIfNecessary(traceId);
        }

        private void onBootstrapMetaReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyAbortIfNecessary(traceId);

            doBootstrapMetaInitialAbortIfNecessary(traceId);
        }

        private void onBootstrapMetaInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedInitial(state);

            doBootstrapMetaReplyResetIfNecessary(traceId);

            bootstrap.doBootstrapCleanup(traceId);
        }

        private void onBootstrapMetaInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                bootstrap.doBootstrapInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }
        }

        private void doBootstrapMetaReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            replyBudget += credit;

            doWindow(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization,
                0L, credit, bootstrap.replyPadding);
        }

        private void doBootstrapMetaReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doBootstrapMetaReplyReset(traceId);
            }
        }

        private void doBootstrapMetaReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);

            doReset(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization);
        }

        private void doBootstrapMetaCleanup(
            long traceId)
        {
            doBootstrapMetaInitialAbortIfNecessary(traceId);
            doBootstrapMetaReplyResetIfNecessary(traceId);
        }
    }

    private final class KafkaBootstrapFetchStream
    {
        private final int leaderId;
        private final int partitionId;
        private final KafkaBootstrapStream bootstrap;

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

        private void doBootstrapInitialBegin(
            long traceId,
            long partitionOffset)
        {
            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(bootstrap.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onBootstrapFetchReply);
            router.setThrottle(initialId, this::onBootstrapFetchReply);
            doBegin(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.topic(bootstrap.topic)
                                     .partition(p -> p.partitionId(partitionId).partitionOffset(OFFSET_EARLIEST))
                                     .deltaType(t -> t.set(KafkaDeltaType.NONE)))
                        .build()
                        .sizeof()));
        }

        private void doBootstrapFetchInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doBootstrapFetchInitialEnd(traceId);
            }
        }

        private void doBootstrapFetchInitialEnd(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doEnd(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void doBootstrapFetchInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doBootstrapFetchInitialAbort(traceId);
            }
        }

        private void doBootstrapFetchInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, bootstrap.resolvedId, initialId, traceId, bootstrap.authorization, EMPTY_EXTENSION);
        }

        private void onBootstrapFetchInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedInitial(state);

            doBootstrapFetchReplyResetIfNecessary(traceId);

            bootstrap.doBootstrapCleanup(traceId);
        }

        private void onBootstrapFetchInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                bootstrap.doBootstrapInitialWindowIfNecessary(traceId, 0L, 0, 0);
            }
        }

        private void onBootstrapFetchReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onBootstrapFetchReplyBegin(begin);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onBootstrapFetchReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onBootstrapFetchReplyAbort(abort);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onBootstrapFetchReplyFlush(flush);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onBootstrapFetchInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onBootstrapFetchInitialWindow(window);
                break;
            default:
                break;
            }
        }

        private void onBootstrapFetchReplyBegin(
            BeginFW begin)
        {
            state = KafkaState.openingReply(state);

            final long traceId = begin.traceId();

            bootstrap.onBootstrapPartitionReady(traceId, partitionId);

            doBootstrapFetchReplyWindow(traceId, 8192); // TODO: consider 0 to avoid receiving FLUSH frames
        }

        private void onBootstrapFetchReplyFlush(
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

                doBootstrapFetchReplyWindow(traceId, reserved);
            }
        }

        private void onBootstrapFetchReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyEndIfNecessary(traceId);

            doBootstrapFetchInitialEndIfNecessary(traceId);
        }

        private void onBootstrapFetchReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            bootstrap.doBootstrapReplyAbortIfNecessary(traceId);

            doBootstrapFetchInitialAbortIfNecessary(traceId);
        }

        private void doBootstrapFetchReplyWindow(
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

        private void doBootstrapFetchReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doBootstrapFetchReplyReset(traceId);
            }
        }

        private void doBootstrapFetchReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);

            doReset(receiver, bootstrap.resolvedId, replyId, traceId, bootstrap.authorization);
        }

        private void doBootstrapFetchCleanup(
            long traceId)
        {
            doBootstrapFetchInitialAbortIfNecessary(traceId);
            doBootstrapFetchReplyResetIfNecessary(traceId);
        }
    }
}
