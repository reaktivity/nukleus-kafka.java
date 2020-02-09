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

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.POSITION_UNSET;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.RETRY_SEGMENT;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaConditionFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaFilterFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheHeaderFW;

public final class KafkaCacheCursorFactory
{
    private final CRC32C checksum;
    private final KafkaFilterCondition nullKeyInfo;

    public KafkaCacheCursorFactory()
    {
        this.checksum = new CRC32C();
        this.nullKeyInfo = initNullKeyInfo(checksum);
    }

    public KafkaCacheCursor newCursor(
        ArrayFW<KafkaFilterFW> filters)
    {
        return new KafkaCacheCursor(asCondition(filters));
    }

    public static final class KafkaCacheCursor
    {
        private final KafkaFilterCondition condition;

        private KafkaCacheSegment segment;
        private long offset;
        private long record;

        KafkaCacheCursor(
            KafkaFilterCondition condition)
        {
            this.condition = condition;
        }

        public void init(
            KafkaCacheSegment segment,
            long offset)
        {
            this.segment = segment;
            this.offset = offset;
            final long record = condition.reset(segment, offset, POSITION_UNSET);
            this.record = record == RETRY_SEGMENT || record == NEXT_SEGMENT ? 0L : record;
        }

        public KafkaCacheEntryFW next(
            KafkaCacheEntryFW cacheEntry)
        {
            KafkaCacheEntryFW nextEntry = null;
            KafkaCacheSegment nextSegment = segment;

            while (nextSegment != null && nextEntry == null)
            {
                final long nextRecord = condition.next(record);

                if (nextRecord == RETRY_SEGMENT)
                {
                    break;
                }
                else if (nextRecord == NEXT_SEGMENT)
                {
                    nextSegment = segment.nextSegment();
                    if (nextSegment != null)
                    {
                        segment = nextSegment;
                        record = condition.reset(segment, offset, POSITION_UNSET);
                    }
                }
                else
                {
                    final int index = KafkaCacheCursorRecord.index(nextRecord);
                    assert index >= 0;

                    final int position = KafkaCacheCursorRecord.value(nextRecord);
                    assert position >= 0;

                    nextEntry = segment.readLog(position, cacheEntry);
                    assert nextEntry != null;

                    if (!condition.test(nextEntry))
                    {
                        nextEntry = null;
                    }

                    this.record = nextRecord;
                }
            }

            return nextEntry;
        }

        public void advance(
            long offset)
        {
            assert offset > this.offset;
            this.offset = offset;
            this.record = KafkaCacheCursorRecord.nextIndex(KafkaCacheCursorRecord.nextValue(record));
        }

        @Override
        public String toString()
        {
            return String.format("%s[offset %d, record %016x, segment %s, condition %s]",
                    getClass().getSimpleName(), offset, record, segment, condition);
        }
    }

    private abstract static class KafkaFilterCondition
    {
        public abstract long reset(
            KafkaCacheSegment segment,
            long offset,
            int position);

        public abstract long next(
            long record);

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
                long record)
            {
                return segment.scanOffset(record);
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
                    final long record = segment.seekOffset(offset);
                    position = KafkaCacheCursorRecord.value(record);
                }

                return segment.seekHash(hash, position);
            }

            @Override
            public final long next(
                long record)
            {
                return segment.scanHash(hash, record);
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
                final ArrayFW<KafkaCacheHeaderFW> headers = cacheEntry.headers();
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
                    final long record = segment.seekOffset(offset);
                    position = KafkaCacheCursorRecord.value(record);
                }

                long nextRecordMax = 0;
                for (int i = 0; i < conditions.size(); i++)
                {
                    final KafkaFilterCondition condition = conditions.get(i);
                    final long nextRecord = condition.reset(segment, offset, position);
                    nextRecordMax = KafkaCacheCursorRecord.maxByValue(nextRecord, nextRecordMax);
                }

                return nextRecordMax;
            }

            @Override
            public long next(
                long record)
            {
                long nextRecord = RETRY_SEGMENT;
                long nextRecordMin = KafkaCacheCursorRecord.previousIndex(record);
                long nextRecordMax;

                do
                {
                    nextRecordMax = KafkaCacheCursorRecord.nextIndex(nextRecordMin);
                    nextRecordMin = Long.MAX_VALUE;

                    for (int i = 0; i < conditions.size(); i++)
                    {
                        final KafkaFilterCondition condition = conditions.get(i);
                        nextRecord = condition.next(nextRecordMax);

                        nextRecordMin = KafkaCacheCursorRecord.minByValue(nextRecord, nextRecordMin);
                        nextRecordMax = KafkaCacheCursorRecord.maxByValue(nextRecord, nextRecordMax);
                    }

                    if (nextRecordMax == RETRY_SEGMENT ||
                        nextRecordMax == NEXT_SEGMENT)
                    {
                        nextRecordMin = nextRecordMax;
                        break;
                    }
                }
                while (KafkaCacheCursorRecord.value(nextRecordMin) != KafkaCacheCursorRecord.value(nextRecordMax));

                return nextRecordMin;
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
                    final long record = segment.seekOffset(offset);
                    position = KafkaCacheCursorRecord.value(record);
                }

                long nextRecordMin = RETRY_SEGMENT;
                for (int i = 0; i < conditions.size(); i++)
                {
                    final KafkaFilterCondition condition = conditions.get(i);
                    final long nextRecord = condition.reset(segment, offset, position);
                    nextRecordMin = KafkaCacheCursorRecord.minByValue(nextRecord, nextRecordMin);
                }

                return nextRecordMin;
            }

            @Override
            public long next(
                long record)
            {
                long nextRecordMin = RETRY_SEGMENT;
                for (int i = 0; i < conditions.size(); i++)
                {
                    final KafkaFilterCondition condition = conditions.get(i);
                    final long nextRecord = condition.next(record);
                    nextRecordMin = KafkaCacheCursorRecord.minByValue(nextRecord, nextRecordMin);
                }

                return nextRecordMin;
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
