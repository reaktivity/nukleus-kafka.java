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
import static org.reaktivity.nukleus.kafka.internal.types.KafkaCapabilities.FETCH_ONLY;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaCapabilities.PRODUCE_ONLY;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW.Builder.DEFAULT_LATEST_OFFSET;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType.HISTORICAL;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType.LIVE;
import static org.reaktivity.nukleus.kafka.internal.types.control.KafkaRouteExFW.Builder.DEFAULT_DELTA_TYPE;
import static org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW.Builder.DEFAULT_MINIMUM;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.LongUnaryOperator;
import java.util.function.ToIntFunction;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.function.MessageConsumer;
import org.reaktivity.nukleus.function.MessageFunction;
import org.reaktivity.nukleus.function.MessagePredicate;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;
import org.reaktivity.nukleus.kafka.internal.budget.MergedBudgetCreditor;
import org.reaktivity.nukleus.kafka.internal.types.Array32FW;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaCapabilities;
import org.reaktivity.nukleus.kafka.internal.types.KafkaConditionFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaConfigFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaFilterFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaNotFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaPartitionFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaValueMatchFW;
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
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaDescribeDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFetchDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaMergedBeginExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaMergedDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaMergedFlushExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaMetaDataExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaResetExFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.ResetFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.WindowFW;
import org.reaktivity.nukleus.route.RouteManager;
import org.reaktivity.nukleus.stream.StreamFactory;

public final class KafkaMergedFactory implements StreamFactory
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

    private static final int ERROR_NOT_LEADER_FOR_PARTITION = 6;
    private static final int ERROR_UNKNOWN = -1;

    private static final int FLAGS_NONE = 0x00;
    private static final int FLAGS_FIN = 0x01;
    private static final int FLAGS_INIT = 0x02;
    private static final int FLAGS_INCOMPLETE = 0x04;
    private static final int FLAGS_INIT_AND_FIN = FLAGS_INIT | FLAGS_FIN;

    private static final int DYNAMIC_PARTITION = -1;

    private static final DirectBuffer EMPTY_BUFFER = new UnsafeBuffer();
    private static final OctetsFW EMPTY_OCTETS = new OctetsFW().wrap(EMPTY_BUFFER, 0, 0);
    private static final Consumer<OctetsFW.Builder> EMPTY_EXTENSION = ex -> {};
    private static final MessageConsumer NO_RECEIVER = (m, b, i, l) -> {};

    private static final List<KafkaMergedFilter> EMPTY_MERGED_FILTERS = Collections.emptyList();

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
    private final DataFW.Builder dataRW = new DataFW.Builder();
    private final EndFW.Builder endRW = new EndFW.Builder();
    private final AbortFW.Builder abortRW = new AbortFW.Builder();
    private final ResetFW.Builder resetRW = new ResetFW.Builder();
    private final WindowFW.Builder windowRW = new WindowFW.Builder();
    private final KafkaBeginExFW.Builder kafkaBeginExRW = new KafkaBeginExFW.Builder();
    private final KafkaDataExFW.Builder kafkaDataExRW = new KafkaDataExFW.Builder();

    private final MessageFunction<RouteFW> wrapRoute = (t, b, i, l) -> routeRO.wrap(b, i, i + l);

    private final MutableInteger partitionCount = new MutableInteger();
    private final MutableInteger initialBudgetMinRW = new MutableInteger();
    private final MutableInteger initialPaddingMaxRW = new MutableInteger();

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
        final KafkaCapabilities capabilities = kafkaMergedBeginEx.capabilities().get();
        final String16FW beginTopic = kafkaMergedBeginEx.topic();
        final String topic = beginTopic != null ? beginTopic.asString() : null;
        final KafkaDeltaType deltaType = kafkaMergedBeginEx.deltaType().get();

        final MessagePredicate filter = (t, b, i, l) ->
        {
            final RouteFW route = wrapRoute.apply(t, b, i, l);
            final KafkaRouteExFW routeEx = route.extension().get(routeExRO::tryWrap);
            final String16FW routeTopic = routeEx != null ? routeEx.topic() : null;
            final KafkaDeltaType routeDeltaType = routeEx != null ? routeEx.deltaType().get() : DEFAULT_DELTA_TYPE;
            return route.localAddress().equals(route.remoteAddress()) &&
                    ((routeTopic == null && beginTopic != null) || Objects.equals(routeTopic, beginTopic)) &&
                    (routeDeltaType == deltaType || deltaType == KafkaDeltaType.NONE);
        };

        MessageConsumer newStream = null;

        final RouteFW route = router.resolve(routeId, authorization, filter, wrapRoute);
        if (route != null)
        {
            final long resolvedId = route.correlationId();
            final ArrayFW<KafkaOffsetFW> partitions = kafkaMergedBeginEx.partitions();

            final KafkaOffsetFW partition = partitions.matchFirst(p -> p.partitionId() == -1L);
            final long defaultOffset = partition != null ? partition.partitionOffset() : HISTORICAL.value();

            final Long2LongHashMap initialOffsetsById = new Long2LongHashMap(-3L);
            partitions.forEach(p ->
            {
                final long partitionId = p.partitionId();
                if (partitionId >= 0L)
                {
                    final long partitionOffset = p.partitionOffset();
                    initialOffsetsById.put(partitionId, partitionOffset);
                }
            });

            newStream = new KafkaMergedStream(
                    sender,
                    routeId,
                    initialId,
                    affinity,
                    authorization,
                    topic,
                    resolvedId,
                    capabilities,
                    initialOffsetsById,
                    defaultOffset,
                    deltaType)::onMergedMessage;
        }

        return newStream;
    }

    private static List<KafkaMergedFilter> asMergedFilters(
        ArrayFW<KafkaFilterFW> filters)
    {
        final List<KafkaMergedFilter> mergedFilters;

        if (filters.isEmpty())
        {
            mergedFilters = EMPTY_MERGED_FILTERS;
        }
        else
        {
            mergedFilters = new ArrayList<>();
            filters.forEach(f -> mergedFilters.add(asMergedFilter(f.conditions())));
        }

        return mergedFilters;
    }

    private static KafkaMergedFilter asMergedFilter(
        ArrayFW<KafkaConditionFW> conditions)
    {
        assert !conditions.isEmpty();

        final List<KafkaMergedCondition> mergedConditions = new ArrayList<>();
        conditions.forEach(c -> mergedConditions.add(asMergedCondition(c)));
        return new KafkaMergedFilter(mergedConditions);
    }

    private static KafkaMergedCondition asMergedCondition(
        KafkaConditionFW condition)
    {
        KafkaMergedCondition mergedCondition = null;

        switch (condition.kind())
        {
        case KafkaConditionFW.KIND_KEY:
            mergedCondition = asMergedCondition(condition.key());
            break;
        case KafkaConditionFW.KIND_HEADER:
            mergedCondition = asMergedCondition(condition.header());
            break;
        case KafkaConditionFW.KIND_NOT:
            mergedCondition = asMergedCondition(condition.not());
            break;
        case KafkaConditionFW.KIND_HEADERS:
            mergedCondition = asMergedCondition(condition.headers());
            break;
        }

        return mergedCondition;
    }

    private static KafkaMergedCondition asMergedCondition(
        KafkaKeyFW key)
    {
        final OctetsFW value = key.value();

        DirectBuffer valueBuffer = null;
        if (value != null)
        {
            valueBuffer = copyBuffer(value);
        }

        return new KafkaMergedCondition.Key(valueBuffer);
    }

    private static KafkaMergedCondition asMergedCondition(
        KafkaHeaderFW header)
    {
        final OctetsFW name = header.name();
        final OctetsFW value = header.value();

        DirectBuffer nameBuffer = null;
        if (name != null)
        {
            nameBuffer = copyBuffer(name);
        }

        DirectBuffer valueBuffer = null;
        if (value != null)
        {
            valueBuffer = copyBuffer(value);
        }

        return new KafkaMergedCondition.Header(nameBuffer, valueBuffer);
    }

    private static KafkaMergedCondition asMergedCondition(
        KafkaHeadersFW headers)
    {
        final OctetsFW name = headers.name();
        final Array32FW<KafkaValueMatchFW> values = headers.values();

        DirectBuffer nameBuffer = null;
        if (name != null)
        {
            nameBuffer = copyBuffer(name);
        }

        DirectBuffer valuesBuffer = null;
        if (values != null)
        {
            valuesBuffer = copyBuffer(values);
        }

        return new KafkaMergedCondition.Headers(nameBuffer, valuesBuffer);
    }

    private static KafkaMergedCondition asMergedCondition(
        KafkaNotFW not)
    {
        return new KafkaMergedCondition.Not(asMergedCondition(not.condition()));
    }

    private static DirectBuffer copyBuffer(
        Flyweight value)
    {
        final DirectBuffer buffer = value.buffer();
        final int index = value.offset();
        final int length = value.sizeof();
        final MutableDirectBuffer copy = new UnsafeBuffer(new byte[length]);
        copy.putBytes(0, buffer, index, length);
        return copy;
    }

    private static int defaultKeyHash(
        KafkaKeyFW key)
    {
        final OctetsFW value = key.value();
        final DirectBuffer buffer = value.buffer();
        final int offset = value.offset();
        final int limit = value.limit();

        final int length = limit - offset;
        final int seed = 0x9747b28c;

        final int m = 0x5bd1e995;
        final int r = 24;

        int h = seed ^ length;
        int length4 = length >> 2;
        for (int i = 0; i < length4; i++)
        {
            final int i4 = offset + i * 4;
            int k = (buffer.getByte(i4 + 0) & 0xff) +
                    ((buffer.getByte(i4 + 1) & 0xff) << 8) +
                    ((buffer.getByte(i4 + 2) & 0xff) << 16) +
                    ((buffer.getByte(i4 + 3) & 0xff) << 24);
            k *= m;
            k ^= k >>> r;
            k *= m;
            h *= m;
            h ^= k;
        }

        final int remaining = length - 4 * length4;
        if (remaining == 3)
        {
            h ^= (buffer.getByte(offset + (length & ~3) + 2) & 0xff) << 16;
        }
        if (remaining >= 2)
        {
            h ^= (buffer.getByte(offset + (length & ~3) + 1) & 0xff) << 8;
        }
        if (remaining >= 1)
        {
            h ^= buffer.getByte(offset + (length & ~3)) & 0xff;
            h *= m;
        }

        h ^= h >>> 13;
        h *= m;
        h ^= h >>> 15;
        return h;
    }

    private static final class KafkaMergedFilter
    {
        private final List<KafkaMergedCondition> conditions;

        private KafkaMergedFilter(
            List<KafkaMergedCondition> conditions)
        {
            this.conditions = conditions;
        }

        @Override
        public int hashCode()
        {
            return conditions.hashCode();
        }

        @Override
        public boolean equals(
            Object obj)
        {
            if (this == obj)
            {
                return true;
            }

            if (!(obj instanceof KafkaMergedFilter))
            {
                return false;
            }

            KafkaMergedFilter that = (KafkaMergedFilter) obj;
            return Objects.equals(this.conditions, that.conditions);
        }
    }

    private abstract static class KafkaMergedCondition
    {
        private static final class Key extends KafkaMergedCondition
        {
            private final DirectBuffer value;

            private Key(
                DirectBuffer value)
            {
                this.value = value;
            }

            @Override
            protected void set(
                KafkaConditionFW.Builder condition)
            {
                condition.key(this::set);
            }

            private void set(
                KafkaKeyFW.Builder key)
            {
                if (value == null)
                {
                    key.length(-1).value((OctetsFW) null);
                }
                else
                {
                    key.length(value.capacity()).value(value, 0, value.capacity());
                }
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(value);
            }

            @Override
            public boolean equals(Object obj)
            {
                if (this == obj)
                {
                    return true;
                }

                if (!(obj instanceof Key))
                {
                    return false;
                }

                Key that = (Key) obj;
                return Objects.equals(this.value, that.value);
            }
        }

        private static final class Header extends KafkaMergedCondition
        {
            private final DirectBuffer name;
            private final DirectBuffer value;

            private Header(
                DirectBuffer name,
                DirectBuffer value)
            {
                this.name = name;
                this.value = value;
            }

            @Override
            protected void set(
                KafkaConditionFW.Builder condition)
            {
                condition.header(this::set);
            }

            private void set(
                KafkaHeaderFW.Builder header)
            {
                if (name == null)
                {
                    header.nameLen(-1).name((OctetsFW) null);
                }
                else
                {
                    header.nameLen(name.capacity()).name(name, 0, name.capacity());
                }

                if (value == null)
                {
                    header.valueLen(-1).value((OctetsFW) null);
                }
                else
                {
                    header.valueLen(value.capacity()).value(value, 0, value.capacity());
                }
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(name, value);
            }

            @Override
            public boolean equals(
                Object o)
            {
                if (this == o)
                {
                    return true;
                }

                if (!(o instanceof Header))
                {
                    return false;
                }

                final Header that = (Header) o;
                return Objects.equals(this.name, that.name) &&
                       Objects.equals(this.value, that.value);
            }
        }

        private static final class Headers extends KafkaMergedCondition
        {
            private final DirectBuffer name;
            private final DirectBuffer values;

            private final Array32FW<KafkaValueMatchFW> valuesRO = new Array32FW<>(new KafkaValueMatchFW());

            private Headers(
                DirectBuffer name,
                DirectBuffer values)
            {
                this.name = name;
                this.values = values;
            }

            @Override
            protected void set(
                KafkaConditionFW.Builder condition)
            {
                condition.headers(this::set);
            }

            private void set(
                KafkaHeadersFW.Builder headers)
            {
                if (name == null)
                {
                    headers.nameLen(-1).name((OctetsFW) null);
                }
                else
                {
                    headers.nameLen(name.capacity()).name(name, 0, name.capacity());
                }

                if (values == null)
                {
                    headers.values(v -> {});
                }
                else
                {
                    Array32FW<KafkaValueMatchFW> values = valuesRO.wrap(this.values, 0, this.values.capacity());
                    headers.values(values);
                }
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(name, values);
            }

            @Override
            public boolean equals(
                Object o)
            {
                if (this == o)
                {
                    return true;
                }

                if (!(o instanceof Headers))
                {
                    return false;
                }

                final Headers that = (Headers) o;
                return Objects.equals(this.name, that.name) &&
                       Objects.equals(this.values, that.values);
            }
        }

        private static final class Not extends KafkaMergedCondition
        {
            private final KafkaMergedCondition nested;

            private Not(
                KafkaMergedCondition nested)
            {
                this.nested = nested;
            }

            @Override
            protected void set(
                KafkaConditionFW.Builder condition)
            {
                condition.not(this::set);
            }

            private void set(
                KafkaNotFW.Builder not)
            {
                not.condition(nested::set);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(nested);
            }

            @Override
            public boolean equals(
                Object o)
            {
                if (this == o)
                {
                    return false;
                }

                if (!(o instanceof Not))
                {
                    return false;
                }

                final Not that = (Not) o;
                return Objects.equals(this.nested, that.nested);
            }
        }

        protected abstract void set(KafkaConditionFW.Builder builder);
    }

    private void doBegin(
        MessageConsumer receiver,
        long routeId,
        long streamId,
        long traceId,
        long authorization,
        long affinity,
        Flyweight.Builder.Visitor extension)
    {
        final BeginFW begin = beginRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .affinity(affinity)
                .extension(b -> b.set(extension))
                .build();

        receiver.accept(begin.typeId(), begin.buffer(), begin.offset(), begin.sizeof());
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
        int padding,
        int minimum)
    {
        final WindowFW window = windowRW.wrap(writeBuffer, 0, writeBuffer.capacity())
                .routeId(routeId)
                .streamId(streamId)
                .traceId(traceId)
                .authorization(authorization)
                .budgetId(budgetId)
                .credit(credit)
                .padding(padding)
                .minimum(minimum)
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

    private static boolean hasFetchCapability(
        KafkaCapabilities capabilities)
    {
        return (capabilities.value() & FETCH_ONLY.value()) != 0;
    }

    private static boolean hasProduceCapability(
        KafkaCapabilities capabilities)
    {
        return (capabilities.value() & PRODUCE_ONLY.value()) != 0;
    }

    private final class KafkaMergedStream
    {
        private final MessageConsumer sender;
        private final long routeId;
        private final long initialId;
        private final long replyId;
        private final long affinity;
        private final long authorization;
        private final String topic;
        private final long resolvedId;
        private final KafkaUnmergedDescribeStream describeStream;
        private final KafkaUnmergedMetaStream metaStream;
        private final List<KafkaUnmergedFetchStream> fetchStreams;
        private final List<KafkaUnmergedProduceStream> produceStreams;
        private final Int2IntHashMap leadersByPartitionId;
        private final Long2LongHashMap latestOffsetByPartitionId;
        private final Long2LongHashMap nextOffsetsById;
        private final long defaultOffset;
        private final KafkaDeltaType deltaType;

        private KafkaOffsetType maximumOffset;
        private List<KafkaMergedFilter> filters;

        private int state;
        private KafkaCapabilities capabilities;

        private int initialBudget;

        private long replyBudgetId;
        private int replyBudget;
        private int replyPadding;
        private int replyMinimum;

        private int nextNullKeyHash;
        private int fetchStreamIndex;
        private long mergedReplyBudgetId = NO_CREDITOR_INDEX;

        private KafkaUnmergedProduceStream producer;

        KafkaMergedStream(
            MessageConsumer sender,
            long routeId,
            long initialId,
            long affinity,
            long authorization,
            String topic,
            long resolvedId,
            KafkaCapabilities capabilities,
            Long2LongHashMap initialOffsetsById,
            long defaultOffset,
            KafkaDeltaType deltaType)
        {
            this.sender = sender;
            this.routeId = routeId;
            this.initialId = initialId;
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.affinity = affinity;
            this.authorization = authorization;
            this.topic = topic;
            this.resolvedId = resolvedId;
            this.capabilities = capabilities;
            this.describeStream = new KafkaUnmergedDescribeStream(this);
            this.metaStream = new KafkaUnmergedMetaStream(this);
            this.fetchStreams = new ArrayList<>();
            this.produceStreams = new ArrayList<>();
            this.leadersByPartitionId = new Int2IntHashMap(-1);
            this.latestOffsetByPartitionId = new Long2LongHashMap(-3);
            this.nextOffsetsById = initialOffsetsById;
            this.defaultOffset = defaultOffset;
            this.deltaType = deltaType;
        }

        private void onMergedMessage(
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
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onMergedInitialData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onMergedInitialEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onMergedInitialAbort(abort);
                break;
            case FlushFW.TYPE_ID:
                final FlushFW flush = flushRO.wrap(buffer, index, index + length);
                onMergedInitialFlush(flush);
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

            router.setThrottle(replyId, this::onMergedMessage);

            final OctetsFW extension = begin.extension();
            final DirectBuffer buffer = extension.buffer();
            final int offset = extension.offset();
            final int limit = extension.limit();

            final KafkaBeginExFW beginEx = kafkaBeginExRO.wrap(buffer, offset, limit);
            final KafkaMergedBeginExFW mergedBeginEx = beginEx.merged();
            final Array32FW<KafkaFilterFW> filters = mergedBeginEx.filters();

            this.maximumOffset = asMaximumOffset(mergedBeginEx.partitions());
            this.filters = asMergedFilters(filters);

            describeStream.doDescribeInitialBegin(traceId);
        }

        private void onMergedInitialData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();

            initialBudget -= reserved;

            if (initialBudget < 0)
            {
                doMergedCleanup(traceId);
            }
            else
            {
                assert hasProduceCapability(capabilities);

                final int flags = data.flags();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();

                if (producer == null)
                {
                    assert (flags & FLAGS_INIT) != FLAGS_NONE;

                    final ExtensionFW dataEx = extension.get(extensionRO::tryWrap);
                    final KafkaDataExFW kafkaDataEx = dataEx != null && dataEx.typeId() == kafkaTypeId ?
                            extension.get(kafkaDataExRO::tryWrap) : null;

                    assert kafkaDataEx != null;
                    assert kafkaDataEx.kind() == KafkaDataExFW.KIND_MERGED;
                    final KafkaMergedDataExFW kafkaMergedDataEx = kafkaDataEx.merged();
                    final KafkaKeyFW key = kafkaMergedDataEx.key();
                    final KafkaOffsetFW partition = kafkaMergedDataEx.partition();
                    final int partitionId = partition.partitionId();
                    final int nextPartitionId = partitionId == DYNAMIC_PARTITION ? nextPartition(key) : partitionId;

                    final KafkaUnmergedProduceStream newProducer = findProducePartitionLeader(nextPartitionId);
                    assert newProducer != null; // TODO
                    this.producer = newProducer;
                }

                assert producer != null;

                producer.doProduceInitialData(traceId, reserved, flags, budgetId, payload, extension);

                if ((flags & FLAGS_FIN) != FLAGS_NONE)
                {
                    this.producer = null;
                }
            }
        }

        private KafkaOffsetType asMaximumOffset(
            Array32FW<KafkaOffsetFW> partitions)
        {
            return partitions.isEmpty() ||
                   partitions.anyMatch(p -> p.latestOffset() != HISTORICAL.value()) ? LIVE : HISTORICAL;
        }

        private int nextPartition(
            KafkaKeyFW key)
        {
            final int partitionCount = leadersByPartitionId.size();
            final int keyHash = key.length() != -1 ? defaultKeyHash(key) : nextNullKeyHash++;
            final int partitionId = partitionCount > 0 ? (0x7fff_ffff & keyHash) % partitionCount : 0;

            return partitionId;
        }

        private void onMergedInitialEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            describeStream.doDescribeInitialEndIfNecessary(traceId);
            metaStream.doMetaInitialEndIfNecessary(traceId);
            fetchStreams.forEach(f -> f.onMergedInitialEnd(traceId));
            produceStreams.forEach(f -> f.doProduceInitialEndIfNecessary(traceId));

            if (fetchStreams.isEmpty())
            {
                doMergedReplyEndIfNecessary(traceId);
            }
        }

        private void onMergedInitialAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            assert !KafkaState.initialClosed(state);
            state = KafkaState.closedInitial(state);

            describeStream.doDescribeInitialAbortIfNecessary(traceId);
            metaStream.doMetaInitialAbortIfNecessary(traceId);
            fetchStreams.forEach(f -> f.onMergedInitialAbort(traceId));
            produceStreams.forEach(f -> f.doProduceInitialEndIfNecessary(traceId));

            if (fetchStreams.isEmpty())
            {
                doMergedReplyAbortIfNecessary(traceId);
            }
        }

        private void onMergedInitialFlush(
            FlushFW flush)
        {
            final long traceId = flush.traceId();
            final OctetsFW extension = flush.extension();
            final ExtensionFW flushEx = extension.get(extensionRO::tryWrap);

            final KafkaFlushExFW kafkaFlushEx = flushEx != null && flushEx.typeId() == kafkaTypeId ?
                    extension.get(kafkaFlushExRO::tryWrap) : null;

            assert kafkaFlushEx != null;
            assert kafkaFlushEx.kind() == KafkaFlushExFW.KIND_MERGED;
            final KafkaMergedFlushExFW kafkaMergedFlushEx = kafkaFlushEx.merged();
            final KafkaCapabilities newCapabilities = kafkaMergedFlushEx.capabilities().get();

            if (capabilities != newCapabilities)
            {
                final Array32FW<KafkaFilterFW> filters = kafkaMergedFlushEx.filters();
                final List<KafkaMergedFilter> newFilters = asMergedFilters(filters);

                this.maximumOffset = asMaximumOffset(kafkaMergedFlushEx.progress());

                if (hasFetchCapability(capabilities) && hasFetchCapability(newCapabilities))
                {
                    assert Objects.equals(this.filters, newFilters);
                }

                if (hasFetchCapability(newCapabilities) && !hasFetchCapability(capabilities))
                {
                    final Long2LongHashMap initialOffsetsById = new Long2LongHashMap(-3L);
                    kafkaMergedFlushEx.progress().forEach(p ->
                    {
                        final long partitionId = p.partitionId();
                        if (partitionId >= 0L)
                        {
                            final long partitionOffset = p.partitionOffset();
                            initialOffsetsById.put(partitionId, partitionOffset);
                        }
                    });

                    nextOffsetsById.clear();
                    nextOffsetsById.putAll(initialOffsetsById);
                }

                this.capabilities = newCapabilities;
                this.filters = newFilters;

                doFetchPartitionsIfNecessary(traceId);
                doProducePartitionsIfNecessary(traceId);
            }
        }

        private void onMergedReplyWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();
            final int minimum = window.minimum();

            replyBudgetId = budgetId;
            replyBudget += credit;
            replyPadding = padding;
            replyMinimum = minimum;

            if (KafkaState.replyOpening(state))
            {
                state = KafkaState.openedReply(state);
                if (mergedReplyBudgetId == NO_CREDITOR_INDEX)
                {
                    mergedReplyBudgetId = creditor.acquire(replyId, budgetId);
                }
            }

            if (mergedReplyBudgetId != NO_CREDITOR_INDEX)
            {
                creditor.credit(traceId, mergedReplyBudgetId, credit);
            }

            doUnmergedFetchReplyWindowsIfNecessary(traceId);
        }

        private void doUnmergedFetchReplyWindowsIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpened(state))
            {
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
        }

        private void onMergedReplyReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedReply(state);
            nextOffsetsById.clear();

            describeStream.doDescribeReplyResetIfNecessary(traceId);
            metaStream.doMetaReplyResetIfNecessary(traceId);
            fetchStreams.forEach(f -> f.onMergedReplyReset(traceId));
            produceStreams.forEach(f -> f.doProduceReplyResetIfNecessary(traceId));

            if (fetchStreams.isEmpty())
            {
                doMergedInitialResetIfNecessary(traceId);
            }
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

            if (!KafkaState.replyOpened(state) && replyBudget > 0)
            {
                state = KafkaState.openedReply(state);
                if (mergedReplyBudgetId == NO_CREDITOR_INDEX)
                {
                    mergedReplyBudgetId = creditor.acquire(replyId, replyBudgetId);
                    creditor.credit(traceId, mergedReplyBudgetId, replyBudget);
                }
            }

            if (capabilities == FETCH_ONLY)
            {
                doBegin(sender, routeId, replyId, traceId, authorization, affinity, httpBeginExToKafka());
            }
            else
            {
                doBegin(sender, routeId, replyId, traceId, authorization, affinity, EMPTY_EXTENSION);
            }

            doUnmergedFetchReplyWindowsIfNecessary(traceId);
        }

        private Flyweight.Builder.Visitor httpBeginExToKafka()
        {
            return (buffer, offset, maxLimit) ->
                kafkaBeginExRW.wrap(buffer, offset, maxLimit)
                              .typeId(kafkaTypeId)
                              .merged(beginExToKafkaMerged())
                              .build()
                              .limit() - offset;
        }

        private Consumer<KafkaMergedBeginExFW.Builder> beginExToKafkaMerged()
        {
            return builder ->
            {
                builder.capabilities(c -> c.set(FETCH_ONLY)).topic(topic);
                latestOffsetByPartitionId.longForEach(
                    (k, v) -> builder.partitionsItem(i -> i.partitionId((int) k).partitionOffset(0L).latestOffset(v)));
            };
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

            Flyweight newKafkaDataEx = EMPTY_OCTETS;

            if (flags != 0x00)
            {
                assert kafkaDataEx != null;

                final KafkaFetchDataExFW kafkaFetchDataEx = kafkaDataEx.fetch();
                final KafkaOffsetFW partition = kafkaFetchDataEx.partition();
                final int partitionId = partition.partitionId();
                final long partitionOffset = partition.partitionOffset();
                final long latestOffset = partition.latestOffset();
                final int deferred = kafkaFetchDataEx.deferred();
                final long timestamp = kafkaFetchDataEx.timestamp();
                final KafkaKeyFW key = kafkaFetchDataEx.key();
                final ArrayFW<KafkaHeaderFW> headers = kafkaFetchDataEx.headers();
                final KafkaDeltaFW delta = kafkaFetchDataEx.delta();

                nextOffsetsById.put(partitionId, partitionOffset + 1);

                newKafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                     .typeId(kafkaTypeId)
                     .merged(f -> f.deferred(deferred)
                                   .timestamp(timestamp)
                                   .partition(p -> p.partitionId(partitionId)
                                                    .partitionOffset(partitionOffset)
                                                    .latestOffset(latestOffset))
                                   .progress(ps -> nextOffsetsById.longForEach((p, o) -> ps.item(i -> i.partitionId((int) p)
                                                                                                       .partitionOffset(o))))
                                   .key(k -> k.length(key.length())
                                              .value(key.value()))
                                   .delta(d -> d.type(t -> t.set(delta.type())).ancestorOffset(delta.ancestorOffset()))
                                   .headers(hs -> headers.forEach(h -> hs.item(i -> i.nameLen(h.nameLen())
                                                                                     .name(h.name())
                                                                                     .valueLen(h.valueLen())
                                                                                     .value(h.value())))))
                     .build();
            }

            doData(sender, routeId, replyId, traceId, authorization, replyBudgetId, reserved,
                flags, payload, newKafkaDataEx);
        }

        private void doMergedReplyEnd(
            long traceId)
        {
            assert !KafkaState.replyClosed(state);
            state = KafkaState.closedReply(state);
            cleanupBudgetCreditorIfNecessary();
            doEnd(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doMergedReplyAbort(
            long traceId)
        {
            assert !KafkaState.replyClosed(state);
            state = KafkaState.closedReply(state);
            cleanupBudgetCreditorIfNecessary();
            doAbort(sender, routeId, replyId, traceId, authorization, EMPTY_EXTENSION);
        }

        private void doMergedInitialWindowIfNecessary(
            long traceId,
            long budgetId)
        {
            int initialBudgetMin = 0;
            int initialPaddingMax = 0;

            // TODO: sync on write, not sync on read
            if (!produceStreams.isEmpty())
            {
                initialBudgetMinRW.value = Integer.MAX_VALUE;
                initialPaddingMaxRW.value = initialPaddingMax;
                produceStreams.forEach(p -> initialBudgetMinRW.value = Math.min(p.initialBudget, initialBudgetMinRW.value));
                produceStreams.forEach(p -> initialPaddingMaxRW.value = Math.max(p.initialPadding, initialPaddingMaxRW.value));
                initialBudgetMin = initialBudgetMinRW.value;
                initialPaddingMax = initialPaddingMaxRW.value;
            }

            final int credit = Math.max(initialBudgetMin - initialBudget, 0);

            if (!KafkaState.initialOpened(state) && !hasProduceCapability(capabilities) || credit > 0)
            {
                doMergedInitialWindow(traceId, budgetId, credit, initialPaddingMax);
            }
        }

        private void doMergedInitialWindow(
            long traceId,
            long budgetId,
            int credit,
            int padding)
        {
            state = KafkaState.openedInitial(state);

            initialBudget += credit;

            doWindow(sender, routeId, initialId, traceId, authorization,
                    budgetId, credit, padding, DEFAULT_MINIMUM);
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
            if (KafkaState.replyOpening(state) &&
                !KafkaState.replyClosed(state))
            {
                describeStream.doDescribeReplyResetIfNecessary(traceId);
                metaStream.doMetaReplyResetIfNecessary(traceId);
                produceStreams.forEach(f -> f.doProduceReplyResetIfNecessary(traceId));

                state = KafkaState.closingReply(state);
                nextOffsetsById.clear();
                fetchStreams.forEach(f -> f.doFetchInitialAbortIfNecessary(traceId));
                if (fetchStreams.isEmpty())
                {
                    doMergedReplyEnd(traceId);
                }
            }
        }

        private void doMergedReplyAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                if (KafkaState.replyOpening(state))
                {
                    doMergedReplyAbort(traceId);
                    describeStream.doDescribeReplyResetIfNecessary(traceId);
                    metaStream.doMetaReplyResetIfNecessary(traceId);
                    fetchStreams.forEach(f -> f.doFetchReplyResetIfNecessary(traceId));
                    produceStreams.forEach(f -> f.doProduceReplyResetIfNecessary(traceId));
                }
                else
                {
                    router.clearThrottle(replyId);
                }
            }
        }

        private void doMergedInitialResetIfNecessary(
            long traceId)
        {
            if (KafkaState.initialOpening(state) &&
                !KafkaState.initialClosed(state))
            {
                describeStream.doDescribeInitialAbortIfNecessary(traceId);
                metaStream.doMetaInitialAbortIfNecessary(traceId);
                produceStreams.forEach(f -> f.doProduceInitialAbortIfNecessary(traceId));

                state = KafkaState.closingInitial(state);
                nextOffsetsById.clear();
                fetchStreams.forEach(f -> f.doFetchReplyResetIfNecessary(traceId));
                creditor.cleanupChild(mergedReplyBudgetId);
                cleanupBudgetCreditorIfNecessary();
                if (fetchStreams.isEmpty())
                {
                    doMergedInitialReset(traceId);
                }
            }
        }

        private void doMergedCleanup(
            long traceId)
        {
            doMergedInitialResetIfNecessary(traceId);
            doMergedReplyAbortIfNecessary(traceId);
        }

        private void cleanupBudgetCreditorIfNecessary()
        {
            if (mergedReplyBudgetId != NO_CREDITOR_INDEX)
            {
                creditor.release(mergedReplyBudgetId);
                mergedReplyBudgetId = NO_CREDITOR_INDEX;
            }
        }

        private void onTopicConfigChanged(
            long traceId,
            ArrayFW<KafkaConfigFW> configs)
        {
            metaStream.doMetaInitialBeginIfNecessary(traceId);
        }

        private void onTopicMetaDataChanged(
            long traceId,
            ArrayFW<KafkaPartitionFW> partitions)
        {
            leadersByPartitionId.clear();
            partitions.forEach(p -> leadersByPartitionId.put(p.partitionId(), p.leaderId()));
            partitionCount.value = 0;
            partitions.forEach(partition -> partitionCount.value++);
            assert leadersByPartitionId.size() == partitionCount.value;

            doFetchPartitionsIfNecessary(traceId);
            doProducePartitionsIfNecessary(traceId);
        }

        private void doFetchPartitionsIfNecessary(
            long traceId)
        {
            if (hasFetchCapability(capabilities))
            {
                final int partitionCount = leadersByPartitionId.size();
                for (int partitionId = 0; partitionId < partitionCount; partitionId++)
                {
                    doFetchPartitionIfNecessary(traceId, partitionId);
                }
                assert fetchStreams.size() >= leadersByPartitionId.size();

                int offsetCount = nextOffsetsById.size();
                for (int partitionId = partitionCount; partitionId < offsetCount; partitionId++)
                {
                    nextOffsetsById.remove(partitionId);
                }
                assert nextOffsetsById.size() <= leadersByPartitionId.size();
            }
        }

        private void doProducePartitionsIfNecessary(
            long traceId)
        {
            if (hasProduceCapability(capabilities))
            {
                final int partitionCount = leadersByPartitionId.size();
                for (int partitionId = 0; partitionId < partitionCount; partitionId++)
                {
                    doProducePartitionIfNecessary(traceId, partitionId);
                }
                assert produceStreams.size() >= leadersByPartitionId.size();
            }
        }

        private void doFetchPartitionIfNecessary(
            long traceId,
            int partitionId)
        {
            final int leaderId = leadersByPartitionId.get(partitionId);
            final long partitionOffset = nextFetchPartitionOffset(partitionId);

            KafkaUnmergedFetchStream leader = findFetchPartitionLeader(partitionId);

            if (leader != null && leader.leaderId != leaderId)
            {
                leader.leaderId = leaderId;
                leader.doFetchInitialBeginIfNecessary(traceId, partitionOffset);
            }

            if (leader == null)
            {
                leader = new KafkaUnmergedFetchStream(partitionId, leaderId, this);
                leader.doFetchInitialBegin(traceId, partitionOffset);
                fetchStreams.add(leader);
            }

            assert leader != null;
            assert leader.partitionId == partitionId;
            assert leader.leaderId == leaderId;
        }

        private void onFetchPartitionLeaderReady(
            long traceId,
            long partitionId,
            long latestOffset)
        {
            nextOffsetsById.putIfAbsent(partitionId, defaultOffset);
            latestOffsetByPartitionId.put(partitionId, latestOffset);

            if (nextOffsetsById.size() == fetchStreams.size())
            {
                doMergedReplyBeginIfNecessary(traceId);

                if (KafkaState.initialClosed(state))
                {
                    doMergedReplyEndIfNecessary(traceId);
                }
            }
        }

        private void onFetchPartitionLeaderError(
            long traceId,
            int partitionId,
            int error)
        {
            if (error == ERROR_NOT_LEADER_FOR_PARTITION)
            {
                final KafkaUnmergedFetchStream leader = findFetchPartitionLeader(partitionId);
                assert leader != null;

                if (nextOffsetsById.containsKey(partitionId) && maximumOffset != HISTORICAL)
                {
                    final long partitionOffset = nextFetchPartitionOffset(partitionId);
                    leader.doFetchInitialBegin(traceId, partitionOffset);
                }
                else
                {
                    fetchStreams.remove(leader);
                    if (fetchStreams.isEmpty())
                    {
                        if (KafkaState.closed(state))
                        {
                            cleanupBudgetCreditorIfNecessary();
                        }

                        if (KafkaState.initialClosing(state))
                        {
                            doMergedInitialResetIfNecessary(traceId);
                        }
                        if (KafkaState.replyClosing(state))
                        {
                            doMergedReplyEndIfNecessary(traceId);
                        }

                    }
                }
            }
            else
            {
                doMergedInitialResetIfNecessary(traceId);
            }
        }

        private long nextFetchPartitionOffset(
            int partitionId)
        {
            long partitionOffset = nextOffsetsById.get(partitionId);
            if (partitionOffset == nextOffsetsById.missingValue())
            {
                partitionOffset = defaultOffset;
            }
            return partitionOffset;
        }

        private KafkaUnmergedFetchStream findFetchPartitionLeader(
            int partitionId)
        {
            KafkaUnmergedFetchStream leader = null;
            for (int index = 0; index < fetchStreams.size(); index++)
            {
                final KafkaUnmergedFetchStream fetchStream = fetchStreams.get(index);
                if (fetchStream.partitionId == partitionId)
                {
                    leader = fetchStream;
                    break;
                }
            }
            return leader;
        }

        private void doProducePartitionIfNecessary(
            long traceId,
            int partitionId)
        {
            final int leaderId = leadersByPartitionId.get(partitionId);

            KafkaUnmergedProduceStream leader = findProducePartitionLeader(partitionId);

            if (leader != null && leader.leaderId != leaderId)
            {
                leader.leaderId = leaderId;
                leader.doProduceInitialBeginIfNecessary(traceId);
            }

            if (leader == null)
            {
                leader = new KafkaUnmergedProduceStream(partitionId, leaderId, this);
                leader.doProduceInitialBegin(traceId);
                produceStreams.add(leader);
            }

            assert leader != null;
            assert leader.partitionId == partitionId;
            assert leader.leaderId == leaderId;
        }

        private void onProducePartitionLeaderReady(
            long traceId,
            long partitionId)
        {
            if (produceStreams.size() == leadersByPartitionId.size())
            {
                if (!KafkaState.initialOpened(state))
                {
                    doMergedInitialWindowIfNecessary(traceId, 0L);
                }

                doMergedReplyBeginIfNecessary(traceId);

                if (KafkaState.initialClosed(state))
                {
                    doMergedReplyEndIfNecessary(traceId);
                }
            }
        }

        private void onProducePartitionLeaderError(
            long traceId,
            int partitionId,
            int error)
        {
            if (error == ERROR_NOT_LEADER_FOR_PARTITION)
            {
                final KafkaUnmergedProduceStream leader = findProducePartitionLeader(partitionId);
                assert leader != null;

                if (leadersByPartitionId.containsKey(partitionId))
                {
                    leader.doProduceInitialBegin(traceId);
                }
                else
                {
                    produceStreams.remove(leader);
                }
            }
            else
            {
                doMergedInitialResetIfNecessary(traceId);
            }
        }

        private KafkaUnmergedProduceStream findProducePartitionLeader(
            int partitionId)
        {
            KafkaUnmergedProduceStream leader = null;
            for (int index = 0; index < produceStreams.size(); index++)
            {
                final KafkaUnmergedProduceStream produceStream = produceStreams.get(index);
                if (produceStream.partitionId == partitionId)
                {
                    leader = produceStream;
                    break;
                }
            }
            return leader;
        }
    }

    private final class KafkaUnmergedDescribeStream
    {
        private final KafkaMergedStream mergedFetch;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver = NO_RECEIVER;

        private int state;

        private int replyBudget;

        private KafkaUnmergedDescribeStream(
            KafkaMergedStream mergedFetch)
        {
            this.mergedFetch = mergedFetch;
        }

        private void doDescribeInitialBegin(
            long traceId)
        {
            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(mergedFetch.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onDescribeReply);
            router.setThrottle(initialId, this::onDescribeReply);
            doBegin(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, 0L,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .describe(m -> m.topic(mergedFetch.topic)
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

            doEnd(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, EMPTY_EXTENSION);
        }

        private void doDescribeInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doDescribeInitialAbort(traceId);
            }
        }

        private void doDescribeInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, mergedFetch.resolvedId, initialId, traceId, mergedFetch.authorization, EMPTY_EXTENSION);
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
                mergedFetch.doMergedCleanup(traceId);
            }
            else
            {
                final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::wrap);
                final KafkaDescribeDataExFW kafkaDescribeDataEx = kafkaDataEx.describe();
                final ArrayFW<KafkaConfigFW> configs = kafkaDescribeDataEx.configs();
                mergedFetch.onTopicConfigChanged(traceId, configs);

                doDescribeReplyWindow(traceId, reserved);
            }
        }

        private void onDescribeReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            mergedFetch.doMergedReplyBeginIfNecessary(traceId);
            mergedFetch.doMergedReplyEndIfNecessary(traceId);

            doDescribeInitialEndIfNecessary(traceId);
        }

        private void onDescribeReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            mergedFetch.doMergedReplyAbortIfNecessary(traceId);

            doDescribeInitialAbortIfNecessary(traceId);
        }

        private void onDescribeInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();

            state = KafkaState.closedInitial(state);

            mergedFetch.doMergedInitialResetIfNecessary(traceId);

            doDescribeReplyResetIfNecessary(traceId);
        }

        private void onDescribeInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                mergedFetch.doMergedInitialWindowIfNecessary(traceId, 0L);
            }
        }

        private void doDescribeReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            replyBudget += credit;

            doWindow(receiver, mergedFetch.resolvedId, replyId, traceId, mergedFetch.authorization,
                0L, credit, mergedFetch.replyPadding, DEFAULT_MINIMUM);
        }

        private void doDescribeReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doDescribeReplyReset(traceId);
            }
        }

        private void doDescribeReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            correlations.remove(replyId);

            doReset(receiver, mergedFetch.resolvedId, replyId, traceId, mergedFetch.authorization);
        }
    }

    private final class KafkaUnmergedMetaStream
    {
        private final KafkaMergedStream mergedFetch;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver = NO_RECEIVER;

        private int state;

        private int replyBudget;

        private KafkaUnmergedMetaStream(
            KafkaMergedStream mergedFetch)
        {
            this.mergedFetch = mergedFetch;
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
            if (KafkaState.initialOpening(state) && !KafkaState.initialClosed(state))
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

                mergedFetch.doMergedInitialWindowIfNecessary(traceId, 0L);
            }
        }

        private void doMetaReplyWindow(
            long traceId,
            int credit)
        {
            state = KafkaState.openedReply(state);

            replyBudget += credit;

            doWindow(receiver, mergedFetch.resolvedId, replyId, traceId, mergedFetch.authorization,
                0L, credit, mergedFetch.replyPadding, DEFAULT_MINIMUM);
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
            correlations.remove(replyId);

            doReset(receiver, mergedFetch.resolvedId, replyId, traceId, mergedFetch.authorization);
        }
    }

    private final class KafkaUnmergedFetchStream
    {
        private final int partitionId;
        private final KafkaMergedStream merged;

        private int leaderId;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver = NO_RECEIVER;

        private int state;

        private int replyBudget;

        private KafkaUnmergedFetchStream(
            int partitionId,
            int leaderId,
            KafkaMergedStream mergedFetch)
        {
            this.partitionId = partitionId;
            this.leaderId = leaderId;
            this.merged = mergedFetch;
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

            this.initialId = supplyInitialId.applyAsLong(merged.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onFetchReply);
            router.setThrottle(initialId, this::onFetchReply);
            doBegin(receiver, merged.resolvedId, initialId, traceId, merged.authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .fetch(f -> f.topic(merged.topic)
                                     .partition(p -> p.partitionId(partitionId)
                                                      .partitionOffset(partitionOffset)
                                                      .latestOffset(merged.maximumOffset.value()))
                                     .filters(fs -> merged.filters.forEach(mf -> fs.item(i -> setFetchFilter(i, mf))))
                                     .deltaType(t -> t.set(merged.deltaType)))
                        .build()
                        .sizeof()));
        }

        private void onMergedInitialEnd(
            long traceId)
        {
            if (!KafkaState.replyClosing(state))
            {
                doFetchInitialEndIfNecessary(traceId);
            }
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

            doEnd(receiver, merged.resolvedId, initialId, traceId, merged.authorization, EMPTY_EXTENSION);
        }

        private void onMergedInitialAbort(
            long traceId)
        {
            if (!KafkaState.replyClosing(state))
            {
                doFetchInitialAbortIfNecessary(traceId);
            }
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

            doAbort(receiver, merged.resolvedId, initialId, traceId, merged.authorization, EMPTY_EXTENSION);
        }

        private void onFetchInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final OctetsFW extension = reset.extension();

            state = KafkaState.closedInitial(state);

            final KafkaResetExFW kafkaResetEx = extension.get(kafkaResetExRO::tryWrap);
            final int defaultError = KafkaState.replyClosed(state) ? ERROR_NOT_LEADER_FOR_PARTITION : -1;
            final int error = kafkaResetEx != null ? kafkaResetEx.error() : defaultError;

            doFetchReplyResetIfNecessary(traceId);

            assert KafkaState.closed(state);

            merged.onFetchPartitionLeaderError(traceId, partitionId, error);
        }

        private void onFetchInitialWindow(
            WindowFW window)
        {
            if (!KafkaState.initialOpened(state))
            {
                final long traceId = window.traceId();

                state = KafkaState.openedInitial(state);

                merged.doMergedInitialWindowIfNecessary(traceId, 0L);
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

            final OctetsFW extension = begin.extension();
            final ExtensionFW beginEx = extensionRO.tryWrap(extension.buffer(), extension.offset(), extension.limit());
            final KafkaBeginExFW kafkaBeginEx = beginEx != null && beginEx.typeId() == kafkaTypeId ?
                kafkaBeginExRO.tryWrap(extension.buffer(), extension.offset(), extension.limit()) : null;

            assert kafkaBeginEx != null;

            final long latestOffset = kafkaBeginEx.fetch().partition().latestOffset();

            merged.onFetchPartitionLeaderReady(traceId, partitionId, latestOffset);

            doFetchReplyWindowIfNecessary(traceId);
        }

        private void onFetchReplyData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();

            assert budgetId == merged.mergedReplyBudgetId;

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                merged.doMergedCleanup(traceId);
            }
            else
            {
                final int flags = data.flags();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();
                final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::tryWrap);

                merged.doMergedReplyData(traceId, flags, reserved, payload, kafkaDataEx);
            }
        }

        private void onFetchReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            if (merged.maximumOffset != HISTORICAL)
            {
                merged.doMergedReplyEndIfNecessary(traceId);
            }
            doFetchInitialEndIfNecessary(traceId);

            merged.onFetchPartitionLeaderError(traceId, partitionId, ERROR_NOT_LEADER_FOR_PARTITION);

            if (merged.maximumOffset == HISTORICAL && merged.fetchStreams.isEmpty())
            {
                merged.doMergedReplyEndIfNecessary(traceId);
            }
        }

        private void onFetchReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            merged.doMergedReplyAbortIfNecessary(traceId);
            doFetchInitialAbortIfNecessary(traceId);

            merged.onFetchPartitionLeaderError(traceId, partitionId, ERROR_NOT_LEADER_FOR_PARTITION);
        }

        private void doFetchReplyWindowIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpened(merged.state) &&
                KafkaState.replyOpening(state) &&
                !KafkaState.replyClosing(state))
            {
                state = KafkaState.openedReply(state);

                final int credit = merged.replyBudget - replyBudget;
                if (credit > 0)
                {
                    replyBudget += credit;

                    doWindow(receiver, merged.resolvedId, replyId, traceId, merged.authorization,
                        merged.mergedReplyBudgetId, credit, merged.replyPadding, merged.replyMinimum);
                }
            }
        }

        private void onMergedReplyReset(
            long traceId)
        {
            if (!KafkaState.initialClosing(state))
            {
                doFetchReplyResetIfNecessary(traceId);
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
            correlations.remove(replyId);

            doReset(receiver, merged.resolvedId, replyId, traceId, merged.authorization);
        }

        private void setFetchFilter(
            KafkaFilterFW.Builder builder,
            KafkaMergedFilter filter)
        {
            filter.conditions.forEach(c -> builder.conditionsItem(ci -> c.set(ci)));
        }
    }

    private final class KafkaUnmergedProduceStream
    {
        private final int partitionId;
        private final KafkaMergedStream merged;

        private int leaderId;

        private long initialId;
        private long replyId;
        private MessageConsumer receiver = NO_RECEIVER;

        private int state;

        private int initialBudget;
        private long initialBudgetId;
        private int initialPadding;

        private int replyBudget;

        private KafkaUnmergedProduceStream(
            int partitionId,
            int leaderId,
            KafkaMergedStream merged)
        {
            this.partitionId = partitionId;
            this.leaderId = leaderId;
            this.merged = merged;
        }

        private void doProduceInitialBeginIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialOpening(state))
            {
                doProduceInitialBegin(traceId);
            }
        }

        private void doProduceInitialBegin(
            long traceId)
        {
            if (KafkaState.closed(state))
            {
                state = 0;
            }

            assert state == 0;

            state = KafkaState.openingInitial(state);

            this.initialId = supplyInitialId.applyAsLong(merged.resolvedId);
            this.replyId = supplyReplyId.applyAsLong(initialId);
            this.receiver = router.supplyReceiver(initialId);

            correlations.put(replyId, this::onProduceReply);
            router.setThrottle(initialId, this::onProduceReply);
            doBegin(receiver, merged.resolvedId, initialId, traceId, merged.authorization, leaderId,
                ex -> ex.set((b, o, l) -> kafkaBeginExRW.wrap(b, o, l)
                        .typeId(kafkaTypeId)
                        .produce(pr -> pr.transaction((String) null) // TODO: default in kafka.idl
                                       .topic(merged.topic)
                                       .partition(part -> part.partitionId(partitionId).
                                           partitionOffset(DEFAULT_LATEST_OFFSET)))
                        .build()
                        .sizeof()));
        }

        private void doProduceInitialData(
            long traceId,
            int reserved,
            int flags,
            long budgetId,
            OctetsFW payload,
            OctetsFW extension)
        {
            initialBudget -= reserved;

            Flyweight newKafkaDataEx = EMPTY_OCTETS;

            if (flags != FLAGS_NONE && flags != FLAGS_INCOMPLETE)
            {
                final ExtensionFW dataEx = extension.get(extensionRO::tryWrap);
                final KafkaDataExFW kafkaDataEx = dataEx != null && dataEx.typeId() == kafkaTypeId ?
                        extension.get(kafkaDataExRO::tryWrap) : null;

                assert kafkaDataEx != null;
                assert kafkaDataEx.kind() == KafkaFlushExFW.KIND_MERGED;
                final KafkaMergedDataExFW kafkaMergedDataEx = kafkaDataEx.merged();
                final int deferred = kafkaMergedDataEx.deferred();
                final long timestamp = kafkaMergedDataEx.timestamp();
                final KafkaOffsetFW partition = kafkaMergedDataEx.partition();
                final KafkaKeyFW key = kafkaMergedDataEx.key();
                final Array32FW<KafkaHeaderFW> headers = kafkaMergedDataEx.headers();

                final int partitionId = partition.partitionId();
                assert partitionId == DYNAMIC_PARTITION || partitionId == this.partitionId;
                final int sequence = (int) partition.partitionOffset();

                switch (flags)
                {
                case FLAGS_INIT_AND_FIN:
                case FLAGS_INIT:
                    newKafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                            .typeId(kafkaTypeId)
                            .produce(pr -> pr.deferred(deferred)
                                           .timestamp(timestamp)
                                           .sequence(sequence)
                                           .key(k -> k.length(key.length())
                                                      .value(key.value()))
                                           .headers(hs -> headers.forEach(h -> hs.item(i -> i.nameLen(h.nameLen())
                                                                                             .name(h.name())
                                                                                             .valueLen(h.valueLen())
                                                                                             .value(h.value())))))
                            .build();
                    break;
                case FLAGS_FIN:
                    newKafkaDataEx = kafkaDataExRW.wrap(extBuffer, 0, extBuffer.capacity())
                            .typeId(kafkaTypeId)
                            .produce(pr -> pr.headers(hs -> headers.forEach(h -> hs.item(i -> i.nameLen(h.nameLen())
                                                                                             .name(h.name())
                                                                                             .valueLen(h.valueLen())
                                                                                             .value(h.value())))))
                            .build();
                    break;
                }
            }

            doData(receiver, merged.resolvedId, initialId, traceId, merged.authorization,
                    budgetId, reserved, flags, payload, newKafkaDataEx);
        }

        private void doProduceInitialEndIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doProduceInitialEnd(traceId);
            }
        }

        private void doProduceInitialEnd(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doEnd(receiver, merged.resolvedId, initialId, traceId, merged.authorization, EMPTY_EXTENSION);
        }

        private void doProduceInitialAbortIfNecessary(
            long traceId)
        {
            if (!KafkaState.initialClosed(state))
            {
                doProduceInitialAbort(traceId);
            }
        }

        private void doProduceInitialAbort(
            long traceId)
        {
            state = KafkaState.closedInitial(state);

            doAbort(receiver, merged.resolvedId, initialId, traceId, merged.authorization, EMPTY_EXTENSION);
        }

        private void onProduceInitialReset(
            ResetFW reset)
        {
            final long traceId = reset.traceId();
            final OctetsFW extension = reset.extension();

            state = KafkaState.closedInitial(state);

            final KafkaResetExFW kafkaResetEx = extension.get(kafkaResetExRO::tryWrap);
            final int error = kafkaResetEx != null ? kafkaResetEx.error() : ERROR_UNKNOWN;

            doProduceReplyResetIfNecessary(traceId);

            assert KafkaState.closed(state);

            merged.onProducePartitionLeaderError(traceId, partitionId, error);
        }

        private void onProduceInitialWindow(
            WindowFW window)
        {
            final long traceId = window.traceId();
            final long budgetId = window.budgetId();
            final int credit = window.credit();
            final int padding = window.padding();

            initialBudget += credit;
            initialBudgetId = budgetId;
            initialPadding = padding;

            state = KafkaState.openedInitial(state);

            merged.doMergedInitialWindowIfNecessary(traceId, initialBudgetId);
        }

        private void onProduceReply(
            int msgTypeId,
            DirectBuffer buffer,
            int index,
            int length)
        {
            switch (msgTypeId)
            {
            case BeginFW.TYPE_ID:
                final BeginFW begin = beginRO.wrap(buffer, index, index + length);
                onProduceReplyBegin(begin);
                break;
            case DataFW.TYPE_ID:
                final DataFW data = dataRO.wrap(buffer, index, index + length);
                onProduceReplyData(data);
                break;
            case EndFW.TYPE_ID:
                final EndFW end = endRO.wrap(buffer, index, index + length);
                onProduceReplyEnd(end);
                break;
            case AbortFW.TYPE_ID:
                final AbortFW abort = abortRO.wrap(buffer, index, index + length);
                onProduceReplyAbort(abort);
                break;
            case ResetFW.TYPE_ID:
                final ResetFW reset = resetRO.wrap(buffer, index, index + length);
                onProduceInitialReset(reset);
                break;
            case WindowFW.TYPE_ID:
                final WindowFW window = windowRO.wrap(buffer, index, index + length);
                onProduceInitialWindow(window);
                break;
            default:
                break;
            }
        }

        private void onProduceReplyBegin(
            BeginFW begin)
        {
            state = KafkaState.openingReply(state);

            final long traceId = begin.traceId();

            merged.onProducePartitionLeaderReady(traceId, partitionId);

            doProduceReplyWindowIfNecessary(traceId);
        }

        private void onProduceReplyData(
            DataFW data)
        {
            final long traceId = data.traceId();
            final long budgetId = data.budgetId();
            final int reserved = data.reserved();

            assert budgetId == merged.mergedReplyBudgetId;

            replyBudget -= reserved;

            if (replyBudget < 0)
            {
                merged.doMergedCleanup(traceId);
            }
            else
            {
                final int flags = data.flags();
                final OctetsFW payload = data.payload();
                final OctetsFW extension = data.extension();
                final KafkaDataExFW kafkaDataEx = extension.get(kafkaDataExRO::tryWrap);

                merged.doMergedReplyData(traceId, flags, reserved, payload, kafkaDataEx);
            }
        }

        private void onProduceReplyEnd(
            EndFW end)
        {
            final long traceId = end.traceId();

            state = KafkaState.closedReply(state);

            merged.doMergedReplyEndIfNecessary(traceId);

            doProduceInitialEndIfNecessary(traceId);
        }

        private void onProduceReplyAbort(
            AbortFW abort)
        {
            final long traceId = abort.traceId();

            state = KafkaState.closedReply(state);

            merged.doMergedReplyAbortIfNecessary(traceId);

            doProduceInitialAbortIfNecessary(traceId);
        }

        private void doProduceReplyWindowIfNecessary(
            long traceId)
        {
            if (KafkaState.replyOpened(merged.state) &&
                KafkaState.replyOpening(state) &&
                !KafkaState.replyClosing(state))
            {
                state = KafkaState.openedReply(state);

                final int credit = merged.replyBudget - replyBudget;
                if (credit > 0)
                {
                    replyBudget += credit;

                    doWindow(receiver, merged.resolvedId, replyId, traceId, merged.authorization,
                        merged.mergedReplyBudgetId, credit, merged.replyPadding, DEFAULT_MINIMUM);
                }
            }
        }

        private void doProduceReplyResetIfNecessary(
            long traceId)
        {
            if (!KafkaState.replyClosed(state))
            {
                doProduceReplyReset(traceId);
            }
        }

        private void doProduceReplyReset(
            long traceId)
        {
            state = KafkaState.closedReply(state);
            correlations.remove(replyId);

            doReset(receiver, merged.resolvedId, replyId, traceId, merged.authorization);
        }
    }
}
