/**
 * Copyright 2016-2019 The Reaktivity Project
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

import static java.util.Objects.requireNonNull;

import java.util.function.LongSupplier;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.AbortFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.BeginFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.DataFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.EndFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaEndExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;

public final class MessageWriter
{
    private final BeginFW.Builder beginRW = new BeginFW.Builder();
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();

    private final KafkaDataExFW.Builder dataExRW = new KafkaDataExFW.Builder();
    private final KafkaEndExFW.Builder endExRW = new KafkaEndExFW.Builder();

    private final OctetsFW messageKeyRO = new OctetsFW();
    private final OctetsFW messageValueRO = new OctetsFW();

    private final MutableDirectBuffer writeBuffer;
    private final LongSupplier supplyTrace;
    private final int kafkaTypeId;

    public MessageWriter(
        MutableDirectBuffer writeBuffer,
        LongSupplier supplyTrace,
        ToIntFunction<String> supplyTypeId)
    {
        this.writeBuffer = requireNonNull(writeBuffer);
        this.supplyTrace = requireNonNull(supplyTrace);
        this.kafkaTypeId = supplyTypeId.applyAsInt(KafkaNukleus.NAME);
    }

    void doBegin(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final Flyweight.Builder.Visitor visitor)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTrace.getAsLong())
                .extension(b -> b.set(visitor))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doData(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final int padding,
        final OctetsFW payload)
    {
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTrace.getAsLong())
                .groupId(0)
                .padding(padding)
                .payload(p -> p.set(payload.buffer(), payload.offset(), payload.sizeof()))
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    void doEnd(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long[] offsets)
    {
        final EndFW end = endRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTrace.getAsLong())
                .extension(b -> b.set(visitKafkaEndEx(offsets)))
                .build();

        receiver.accept(end.typeId(), end.buffer(), end.offset(), end.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaEndEx(
        long[] fetchOffsets)
    {
        return (b, o, l) ->
        {
            return endExRW.wrap(b, o, l)
                    .typeId(kafkaTypeId)
                    .fetchOffsets(a ->
                    {
                        if (fetchOffsets != null)
                        {
                            for (int i=0; i < fetchOffsets.length; i++)
                            {
                                final long offset = fetchOffsets[i];
                                a.item(p -> p.set(offset));

                            }
                        }
                    })
                    .build()
                    .sizeof();
        };
    }

    void doAbort(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId)
    {
        final AbortFW abort = abortRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTrace.getAsLong())
                .build();

        receiver.accept(abort.typeId(), abort.buffer(), abort.offset(), abort.sizeof());
    }

    void doWindow(
        final MessageConsumer sender,
        final long routeId,
        final long streamId,
        final int credit,
        final int padding,
        final long groupId)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTrace.getAsLong())
                .credit(credit)
                .padding(padding)
                .groupId(groupId)
                .build();

        sender.accept(window.typeId(), window.buffer(), window.offset(), window.sizeof());
    }

    void doReset(
        final MessageConsumer sender,
        final long routeId,
        final long streamId)
    {
        final ResetFW reset = resetRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTrace.getAsLong())
                .build();

        sender.accept(reset.typeId(), reset.buffer(), reset.offset(), reset.sizeof());
    }

    void doKafkaBegin(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final byte[] extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(supplyTrace.getAsLong())
                .extension(b -> b.set(extension))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
    }

    void doKafkaData(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId,
        final int padding,
        final byte flags,
        final DirectBuffer messageKey,
        final long timestamp,
        final DirectBuffer messageValue,
        final int messageValueLimit,
        final Long2LongHashMap fetchOffsets)
    {
        OctetsFW key = messageKey == null ? null : messageKeyRO.wrap(messageKey, 0, messageKey.capacity());
        OctetsFW value = messageValue == null ? null : messageValueRO.wrap(messageValue, 0, messageValueLimit);
        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .flags(flags)
                .groupId(0)
                .padding(padding)
                .payload(value)
                .extension(e -> e.set(visitKafkaDataEx(timestamp, fetchOffsets, key)))
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    void doKafkaDataContinuation(
        final MessageConsumer receiver,
        final long routeId,
        final long streamId,
        final long traceId,
        final int padding,
        final byte flags,
        final DirectBuffer messageValue,
        final int messageValueOffset,
        final int messageValueLimit)
    {
        OctetsFW value = messageValue == null ? null : messageValueRO.wrap(messageValue, messageValueOffset, messageValueLimit);

        final DataFW data = dataRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .trace(traceId)
                .flags(flags)
                .groupId(0)
                .padding(padding)
                .payload(value)
                .build();

        receiver.accept(data.typeId(), data.buffer(), data.offset(), data.sizeof());
    }

    private Flyweight.Builder.Visitor visitKafkaDataEx(
        long timestamp,
        Long2LongHashMap fetchOffsets,
        final OctetsFW messageKey)
    {
        return (b, o, l) ->
        {
            return dataExRW.wrap(b, o, l)
                    .typeId(kafkaTypeId)
                    .timestamp(timestamp)
                    .fetchOffsets(a ->
                    {
                        if (fetchOffsets.size() == 1)
                        {
                            final long offset = fetchOffsets.values().iterator().nextValue();
                            a.item(p -> p.set(offset));
                        }
                        else
                        {
                            int partition = -1;
                            while (fetchOffsets.get(++partition) != fetchOffsets.missingValue())
                            {
                                final long offset = fetchOffsets.get(partition);
                                a.item(p -> p.set(offset));
                            }
                        }
                    })
                    .messageKey(messageKey)
                    .build()
                    .sizeof();
        };
    }
}
