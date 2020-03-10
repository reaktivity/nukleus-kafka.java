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

import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.StringFW;
import org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW;
import org.reaktivity.nukleus.kafka.internal.types.control.RouteFW;
import org.reaktivity.nukleus.kafka.internal.types.control.UnrouteFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.Address;
import org.reaktivity.nukleus.route.AddressFactory;
import org.reaktivity.nukleus.route.RouteManager;

public class KafkaCacheServerAddressFactory implements AddressFactory
{
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};

    private final RouteFW routeRO = new RouteFW();
    private final UnrouteFW unrouteRO = new UnrouteFW();
    private final KafkaRouteExFW kafkaRouteExRO = new KafkaRouteExFW();

    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();

    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();

    private final int kafkaTypeId;
    private final RouteManager router;
    private final LongSupplier supplyTraceId;
    private final LongUnaryOperator supplyInitialId;
    private final LongUnaryOperator supplyReplyId;
    private final MutableDirectBuffer writeBuffer;
    private final Long2ObjectHashMap<MessageConsumer> correlations;

    private final Long2ObjectHashMap<KafkaAddressStream> streamsByRouteId;

    public KafkaCacheServerAddressFactory(
        KafkaConfiguration config,
        RouteManager router,
        MutableDirectBuffer writeBuffer,
        ToIntFunction<String> supplyTypeId,
        LongSupplier supplyTraceId,
        LongUnaryOperator supplyInitialId,
        LongUnaryOperator supplyReplyId,
        Long2ObjectHashMap<MessageConsumer> correlations)
    {
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
        this.router = router;
        this.writeBuffer = writeBuffer;
        this.supplyTraceId = supplyTraceId;
        this.supplyInitialId = supplyInitialId;
        this.supplyReplyId = supplyReplyId;
        this.correlations = correlations;
        this.streamsByRouteId = new Long2ObjectHashMap<>();
    }

    @Override
    public KafkaCacheServerAddress newAddress(
        String localName)
    {
        return new KafkaCacheServerAddress(localName);
    }

    private void onCacheServerMessage(
        int msgTypeId,
        DirectBuffer buffer,
        int index,
        int length)
    {
        switch (msgTypeId)
        {
        case RouteFW.TYPE_ID:
            final RouteFW route = routeRO.wrap(buffer, index, index + length);
            onCacheServerRouted(route);
            break;
        case UnrouteFW.TYPE_ID:
            final UnrouteFW unroute = unrouteRO.wrap(buffer, index, index + length);
            onCacheServerUnrouted(unroute);
            break;
        }
    }

    private void onCacheServerRouted(
        RouteFW route)
    {
        final StringFW localAddress = route.localAddress();
        final StringFW remoteAddress = route.remoteAddress();
        final long authorization = route.authorization();
        final OctetsFW extension = route.extension();
        final KafkaRouteExFW kafkaRouteEx = extension.get(kafkaRouteExRO::tryWrap);

        if (remoteAddress.equals(localAddress) &&
            kafkaRouteEx != null)
        {
            final String topic = kafkaRouteEx.topic().asString();

            if (topic != null)
            {
                final long routeId = route.correlationId();
                assert !streamsByRouteId.containsKey(routeId);

                final KafkaAddressStream stream = new KafkaAddressStream(routeId, authorization, topic);
                streamsByRouteId.put(routeId, stream);

                stream.doKafkaInitialBegin();
            }
        }
    }

    private void onCacheServerUnrouted(
        UnrouteFW unroute)
    {
        final long routeId = unroute.correlationId();
        final KafkaAddressStream stream = streamsByRouteId.remove(routeId);

        if (stream != null)
        {
            stream.doKafkaInitialEnd();
        }
    }

    private final class KafkaAddressStream
    {
        private final long routeId;
        private final long authorization;
        private final long initialId;
        private final long replyId;
        private final MessageConsumer receiver;
        private final String topic;

        private int state;

        private KafkaAddressStream(
            long routeId,
            long authorization,
            String topic)
        {
            this.routeId = routeId;
            this.authorization = authorization;
            this.initialId = supplyInitialId.applyAsLong(routeId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);
            this.topic = topic;
        }

        private void doKafkaInitialBegin()
        {
            final long traceId = supplyTraceId.getAsLong();

            state = KafkaState.openingInitial(state);

            correlations.put(replyId, this::onKafkaReply);
            router.setThrottle(initialId, this::onKafkaReply);
            doBegin(receiver, routeId, initialId, traceId, authorization, 0,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .bootstrap(bs -> bs.topic(topic))
                        .build()
                        .sizeof()));
        }

        private void doKafkaInitialEnd()
        {
            final long traceId = supplyTraceId.getAsLong();

            state = KafkaState.closedInitial(state);

            correlations.remove(replyId);
            doEnd(receiver, routeId, initialId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void onKafkaReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            final long traceId = supplyTraceId.getAsLong();

            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                state = KafkaState.openedReply(state);
                doKafkaReplyWindow(traceId, 0, 0);
                break;
            case EndFW.TYPE_ID:
            case AbortFW.TYPE_ID:
                assert KafkaState.initialClosed(state) : "reply closed unexpectedly";
                state = KafkaState.closedReply(state);
                break;
            case ResetFW.TYPE_ID:
                correlations.remove(replyId);
                assert KafkaState.replyClosing(state) : "initial closed unexpectedly";
                state = KafkaState.closedInitial(state);
                doKafkaReplyReset(traceId);
                break;
            case WindowFW.TYPE_ID:
                state = KafkaState.openedInitial(state);
                break;
            }
        }

        private void doKafkaReplyReset(
            long traceId)
        {
            doReset(receiver, routeId, replyId, traceId, authorization);
        }

        private void doKafkaReplyWindow(
            long traceId,
            int credit,
            int padding)
        {
            doWindow(receiver, routeId, replyId, traceId, authorization, 0L, credit, padding);
        }
    }

    public final class KafkaCacheServerAddress implements Address
    {
        private final String name;

        private KafkaCacheServerAddress(
            String name)
        {
            this.name = name;
        }

        @Override
        public String name()
        {
            return name;
        }

        @Override
        public String nukleus()
        {
            return KafkaNukleus.NAME;
        }

        @Override
        public MessageConsumer routeHandler()
        {
            return KafkaCacheServerAddressFactory.this::onCacheServerMessage;
        }
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
}
