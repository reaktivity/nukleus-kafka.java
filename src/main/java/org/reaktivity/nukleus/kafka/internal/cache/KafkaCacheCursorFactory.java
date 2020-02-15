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
package org.reaktivity.nukleus.kafka.internal.cache;

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.RETRY_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorIndex;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.cursorValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.maxByValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.minByValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.nextIndex;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.nextValue;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheCursorRecord.previousIndex;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.POSITION_UNSET;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.LongHashSet;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegmentFactory.KafkaCacheSegment;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaConditionFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType;
import org.reaktivity.nukleus.kafka.internal.types.KafkaFilterFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheDeltaFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;

public final class KafkaCacheCursorFactory
{
    private final KafkaCacheDeltaFW deltaRO = new KafkaCacheDeltaFW();

    private final MutableDirectBuffer writeBuffer;
    private final CRC32C checksum;
    private final KafkaFilterCondition nullKeyInfo;

    public KafkaCacheCursorFactory(
        MutableDirectBuffer writeBuffer)
    {
        this.writeBuffer = writeBuffer;
        this.checksum = new CRC32C();
        this.nullKeyInfo = initNullKeyInfo(checksum);
    }

    public KafkaCacheCursor newCursor(
        ArrayFW<KafkaFilterFW> filters,
        KafkaDeltaType deltaType)
    {
        return new KafkaCacheCursor(asCondition(filters), deltaType);
    }

    public final class KafkaCacheCursor
    {
        private final KafkaFilterCondition condition;
        private final KafkaDeltaType deltaType;
        private final LongHashSet deltaKeyOffsets; // TODO: bounded LongHashCache, evict -> discard

        private KafkaCacheSegment segment;
        private long offset;
        private long cursor;

        KafkaCacheCursor(
            KafkaFilterCondition condition,
            KafkaDeltaType deltaType)
        {
            this.condition = condition;
            this.deltaType = deltaType;
            this.deltaKeyOffsets = new LongHashSet();
        }

        public void init(
            KafkaCacheSegment segment,
            long offset)
        {
            this.segment = segment;
            this.offset = offset;
            final long cursor = condition.reset(segment, offset, POSITION_UNSET);
            this.cursor = cursor == RETRY_SEGMENT || cursor == NEXT_SEGMENT ? 0L : cursor;
        }

        public KafkaCacheEntryFW next(
            KafkaCacheEntryFW cacheEntry)
        {
            KafkaCacheEntryFW nextEntry = null;
            KafkaCacheSegment nextSegment = segment;

            while (nextSegment != null && nextEntry == null)
            {
                final long nextCursor = condition.next(cursor);

                if (nextCursor == RETRY_SEGMENT)
                {
                    break;
                }
                else if (nextCursor == NEXT_SEGMENT)
                {
                    nextSegment = segment.nextSegment();
                    if (nextSegment != null)
                    {
                        segment = nextSegment;
                        cursor = condition.reset(segment, offset, POSITION_UNSET);
                    }
                }
                else
                {
                    final int cursorIndex = cursorIndex(nextCursor);
                    assert cursorIndex >= 0;

                    final int position = cursorValue(nextCursor);
                    assert position >= 0;

                    nextEntry = segment.readLog(position, cacheEntry);
                    assert nextEntry != null;

                    if (!condition.test(nextEntry))
                    {
                        nextEntry = null;
                    }

                    if (nextEntry != null && deltaType != KafkaDeltaType.NONE)
                    {
                        final long ancestorOffset = nextEntry.ancestor();

                        if (nextEntry.valueLen() == -1)
                        {
                            deltaKeyOffsets.remove(ancestorOffset);
                        }
                        else
                        {
                            final long partitionOffset = nextEntry.offset$();
                            final int deltaPosition = nextEntry.deltaPosition();

                            if (ancestorOffset != -1)
                            {
                                if (deltaPosition != -1 && deltaKeyOffsets.remove(ancestorOffset))
                                {
                                    final KafkaCacheDeltaFW delta = segment.readDelta(deltaPosition, deltaRO);

                                    final DirectBuffer entryBuffer = nextEntry.buffer();
                                    final KafkaKeyFW key = nextEntry.key();
                                    final int entryOffset = nextEntry.offset();
                                    final ArrayFW<KafkaHeaderFW> headers = nextEntry.headers();

                                    final int sizeofEntryHeader = key.limit() - nextEntry.offset();
                                    writeBuffer.putBytes(0, entryBuffer, entryOffset, sizeofEntryHeader);
                                    writeBuffer.putBytes(sizeofEntryHeader, delta.buffer(), delta.offset(), delta.sizeof());
                                    writeBuffer.putBytes(sizeofEntryHeader + delta.sizeof(),
                                            headers.buffer(), headers.offset(), headers.sizeof());

                                    final int sizeofEntry = sizeofEntryHeader + delta.sizeof() + headers.sizeof();
                                    nextEntry = cacheEntry.wrap(writeBuffer, 0, sizeofEntry);
                                }
                                else
                                {
                                    // TODO: consider moving message to next segment if delta exceeds size limit instead
                                    //       still need to handle implicit snapshot case
                                    writeBuffer.putBytes(0, nextEntry.buffer(), nextEntry.offset(), nextEntry.sizeof());
                                    writeBuffer.putLong(KafkaCacheEntryFW.FIELD_OFFSET_ANCESTOR, -1L);

                                    nextEntry = cacheEntry.wrap(writeBuffer, 0, writeBuffer.capacity());
                                }
                            }

                            deltaKeyOffsets.add(partitionOffset);
                        }

                    }

                    this.cursor = nextCursor;
                }
            }

            return nextEntry;
        }

        public void advance(
            long offset)
        {
            assert offset > this.offset;
            this.offset = offset;
            this.cursor = nextIndex(nextValue(cursor));
        }

        @Override
        public String toString()
        {
            return String.format("%s[offset %d, cursor %016x, segment %s, condition %s]",
                    getClass().getSimpleName(), offset, cursor, segment, condition);
        }
    }

    private abstract static class KafkaFilterCondition
    {
        public abstract long reset(
            KafkaCacheSegment segment,
            long offset,
            int position);

        public abstract long next(
            long cursor);

        public abstract boolean test(
            KafkaCacheEntryFW cacheEntry);

        private static final class None extends KafkaFilterCondition
        {
            private KafkaCacheSegment segment;

            @Override
            public long reset(
                KafkaCacheSegment segment,
                long offset,
                int position)
            {
                this.segment = segment;
                assert position == POSITION_UNSET;
                return segment.seekOffset(offset);
            }

            @Override
            public long next(
                long cursor)
            {
                return segment.scanOffset(cursor);
            }

            @Override
            public boolean test(
                KafkaCacheEntryFW cacheEntry)
            {
                return cacheEntry != null;
            }

            @Override
            public String toString()
            {
                return String.format("%s[]", getClass().getSimpleName());
            }
        }

        private abstract static class Equals extends KafkaFilterCondition
        {
            private final int hash;
            private final DirectBuffer value;
            private final UnsafeBuffer comparable;

            private KafkaCacheSegment segment;

            @Override
            public final long reset(
                KafkaCacheSegment segment,
                long offset,
                int position)
            {
                this.segment = segment;

                if (position == POSITION_UNSET)
                {
                    final long cursor = segment.seekOffset(offset);
                    position = cursorValue(cursor);
                }

                return segment.seekHash(hash, position);
            }

            @Override
            public final long next(
                long cursor)
            {
                return segment.scanHash(hash, cursor);
            }

            @Override
            public final String toString()
            {
                return String.format("%s[%08x]", getClass().getSimpleName(), hash);
            }

            protected Equals(
                CRC32C checksum,
                DirectBuffer buffer,
                int index,
                int length)
            {
                this.value = copyBuffer(buffer, index, length);
                this.hash = computeHash(buffer, index, length, checksum);
                this.comparable = new UnsafeBuffer();
            }

            protected final boolean test(
                Flyweight header)
            {
                comparable.wrap(header.buffer(), header.offset(), header.sizeof());
                return comparable.compareTo(value) == 0;
            }
        }

        private static final class Key extends Equals
        {
            private Key(
                CRC32C checksum,
                KafkaKeyFW key)
            {
                super(checksum, key.buffer(), key.offset(), key.sizeof());
            }

            @Override
            public boolean test(
                KafkaCacheEntryFW cacheEntry)
            {
                return test(cacheEntry.key());
            }
        }

        private static final class Header extends Equals
        {
            private final MutableBoolean match;

            private Header(
                CRC32C checksum,
                KafkaHeaderFW header)
            {
                super(checksum, header.buffer(), header.offset(), header.sizeof());
                this.match = new MutableBoolean();
            }

            @Override
            public boolean test(
                KafkaCacheEntryFW cacheEntry)
            {
                final ArrayFW<KafkaHeaderFW> headers = cacheEntry.headers();
                match.value = false;
                headers.forEach(header -> match.value |= test(header));
                return match.value;
            }
        }

        private static final class And extends KafkaFilterCondition
        {
            private final List<KafkaFilterCondition> conditions;

            private And(
                List<KafkaFilterCondition> conditions)
            {
                this.conditions = conditions;
            }

            @Override
            public long reset(
                KafkaCacheSegment segment,
                long offset,
                int position)
            {
                if (position == POSITION_UNSET)
                {
                    final long cursor = segment.seekOffset(offset);
                    position = cursorValue(cursor);
                }

                long nextCursorMax = 0;
                for (int i = 0; i < conditions.size(); i++)
                {
                    final KafkaFilterCondition condition = conditions.get(i);
                    final long nextCursor = condition.reset(segment, offset, position);
                    nextCursorMax = maxByValue(nextCursor, nextCursorMax);
                }

                return nextCursorMax;
            }

            @Override
            public long next(
                long cursor)
            {
                long nextCursor = RETRY_SEGMENT;
                long nextCursorMin = previousIndex(cursor);
                long nextCursorMax;

                do
                {
                    nextCursorMax = nextIndex(nextCursorMin);
                    nextCursorMin = Long.MAX_VALUE;

                    for (int i = 0; i < conditions.size(); i++)
                    {
                        final KafkaFilterCondition condition = conditions.get(i);
                        nextCursor = condition.next(nextCursorMax);

                        nextCursorMin = minByValue(nextCursor, nextCursorMin);
                        nextCursorMax = maxByValue(nextCursor, nextCursorMax);
                    }

                    if (nextCursorMax == RETRY_SEGMENT ||
                        nextCursorMax == NEXT_SEGMENT)
                    {
                        nextCursorMin = nextCursorMax;
                        break;
                    }
                }
                while (cursorValue(nextCursorMin) != cursorValue(nextCursorMax));

                return nextCursorMin;
            }

            @Override
            public boolean test(
                KafkaCacheEntryFW cacheEntry)
            {
                boolean accept = true;
                for (int i = 0; accept && i < conditions.size(); i++)
                {
                    final KafkaFilterCondition condition = conditions.get(i);
                    accept &= condition.test(cacheEntry);
                }
                return accept;
            }

            @Override
            public String toString()
            {
                return String.format("%s%s", getClass().getSimpleName(), conditions);
            }
        }

        private static final class Or extends KafkaFilterCondition
        {
            private final List<KafkaFilterCondition> conditions;

            private Or(
                List<KafkaFilterCondition> conditions)
            {
                this.conditions = conditions;
            }

            @Override
            public long reset(
                KafkaCacheSegment segment,
                long offset,
                int position)
            {
                if (position == POSITION_UNSET)
                {
                    final long cursor = segment.seekOffset(offset);
                    position = cursorValue(cursor);
                }

                long nextCursorMin = RETRY_SEGMENT;
                for (int i = 0; i < conditions.size(); i++)
                {
                    final KafkaFilterCondition condition = conditions.get(i);
                    final long nextCursor = condition.reset(segment, offset, position);
                    nextCursorMin = minByValue(nextCursor, nextCursorMin);
                }

                return nextCursorMin;
            }

            @Override
            public long next(
                long cursor)
            {
                long nextCursorMin = RETRY_SEGMENT;
                for (int i = 0; i < conditions.size(); i++)
                {
                    final KafkaFilterCondition condition = conditions.get(i);
                    final long nextCursor = condition.next(cursor);
                    nextCursorMin = minByValue(nextCursor, nextCursorMin);
                }

                return nextCursorMin;
            }

            @Override
            public boolean test(
                KafkaCacheEntryFW cacheEntry)
            {
                boolean accept = false;
                for (int i = 0; !accept && i < conditions.size(); i++)
                {
                    final KafkaFilterCondition condition = conditions.get(i);
                    accept |= condition.test(cacheEntry);
                }
                return accept;
            }

            @Override
            public String toString()
            {
                return String.format("%s%s", getClass().getSimpleName(), conditions);
            }
        }

        private static DirectBuffer copyBuffer(
            DirectBuffer buffer,
            int index,
            int length)
        {
            UnsafeBuffer copy = new UnsafeBuffer(new byte[length]);
            copy.putBytes(0, buffer, index, length);
            return copy;
        }

        private static int computeHash(
            DirectBuffer buffer,
            int index,
            int length,
            CRC32C checksum)
        {
            final ByteBuffer byteBuffer = buffer.byteBuffer();
            assert byteBuffer != null;
            byteBuffer.clear();
            byteBuffer.position(index);
            byteBuffer.limit(index + length);
            checksum.reset();
            checksum.update(byteBuffer);
            return (int) checksum.getValue();
        }
    }

    private KafkaFilterCondition asCondition(
        ArrayFW<KafkaFilterFW> filters)
    {
        KafkaFilterCondition condition;
        if (filters.isEmpty())
        {
            condition = new KafkaFilterCondition.None();
        }
        else
        {
            final List<KafkaFilterCondition> asConditions = new ArrayList<>();
            filters.forEach(f -> asConditions.add(asCondition(f)));
            condition = asConditions.size() == 1 ? asConditions.get(0) : new KafkaFilterCondition.Or(asConditions);
        }
        return condition;
    }

    private KafkaFilterCondition asCondition(
        KafkaFilterFW filter)
    {
        final ArrayFW<KafkaConditionFW> conditions = filter.conditions();
        assert !conditions.isEmpty();
        List<KafkaFilterCondition> asConditions = new ArrayList<>();
        conditions.forEach(c -> asConditions.add(asCondition(c)));
        return asConditions.size() == 1 ? asConditions.get(0) : new KafkaFilterCondition.And(asConditions);
    }

    private KafkaFilterCondition asCondition(
        KafkaConditionFW condition)
    {
        KafkaFilterCondition asCondition = null;

        switch (condition.kind())
        {
        case KafkaConditionFW.KIND_KEY:
            asCondition = asKeyCondition(condition.key());
            break;
        case KafkaConditionFW.KIND_HEADER:
            asCondition = asHeaderCondition(condition.header());
            break;
        }

        assert asCondition != null;
        return asCondition;
    }

    private KafkaFilterCondition asKeyCondition(
        KafkaKeyFW key)
    {
        final OctetsFW value = key.value();

        return value == null ? nullKeyInfo : new KafkaFilterCondition.Key(checksum, key);
    }

    private KafkaFilterCondition asHeaderCondition(
        KafkaHeaderFW header)
    {
        return new KafkaFilterCondition.Header(checksum, header);
    }

    private static KafkaFilterCondition.Key initNullKeyInfo(
        CRC32C checksum)
    {
        final KafkaKeyFW nullKeyRO = new KafkaKeyFW.Builder()
                .wrap(new UnsafeBuffer(ByteBuffer.allocate(5)), 0, 5)
                .length(-1)
                .value((OctetsFW) null)
                .build();
        return new KafkaFilterCondition.Key(checksum, nullKeyRO);
    }
}
