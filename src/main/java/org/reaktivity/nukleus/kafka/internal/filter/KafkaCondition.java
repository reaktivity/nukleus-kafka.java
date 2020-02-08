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
package org.reaktivity.nukleus.kafka.internal.filter;

import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.NEXT_SEGMENT;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.POSITION_UNSET;
import static org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment.RETRY_SEGMENT;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.CRC32C;

import org.agrona.DirectBuffer;
import org.agrona.collections.MutableBoolean;
import org.agrona.concurrent.UnsafeBuffer;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheSegment;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaIndexRecord;
import org.reaktivity.nukleus.kafka.internal.types.ArrayFW;
import org.reaktivity.nukleus.kafka.internal.types.Flyweight;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaKeyFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheEntryFW;
import org.reaktivity.nukleus.kafka.internal.types.cache.KafkaCacheHeaderFW;

public abstract class KafkaCondition
{
    public abstract long reset(
        KafkaCacheSegment segment,
        long offset,
        int position);

    public abstract long next(
        long record);

    public abstract boolean test(
        KafkaCacheEntryFW cacheEntry);

    public static final class None extends KafkaCondition
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
    }

    public abstract static class Equals extends KafkaCondition
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
                position = KafkaIndexRecord.value(record);
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
            DirectBuffer buffer)
        {
            this(checksum, buffer, 0, buffer.capacity());
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

    public static final class Key extends Equals
    {
        public Key(
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

    public static final class Header extends Equals
    {
        private final MutableBoolean match;

        public Header(
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

    public static final class And extends KafkaCondition
    {
        private final List<KafkaCondition> conditions;

        public And(
            List<KafkaCondition> conditions)
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
                position = KafkaIndexRecord.value(record);
            }

            long nextRecordMax = 0;
            for (int i = 0; i < conditions.size(); i++)
            {
                final KafkaCondition condition = conditions.get(i);
                final long nextRecord = condition.reset(segment, offset, position);
                nextRecordMax = KafkaIndexRecord.maxByValue(nextRecord, nextRecordMax);
            }

            return nextRecordMax;
        }

        @Override
        public long next(
            long record)
        {
            long nextRecord = RETRY_SEGMENT;
            long nextRecordMin = KafkaIndexRecord.previousIndex(record);
            long nextRecordMax;

            do
            {
                nextRecordMax = KafkaIndexRecord.nextIndex(nextRecordMin);
                nextRecordMin = Long.MAX_VALUE;

                for (int i = 0; i < conditions.size(); i++)
                {
                    final KafkaCondition condition = conditions.get(i);
                    nextRecord = condition.next(nextRecordMax);

                    nextRecordMin = KafkaIndexRecord.minByValue(nextRecord, nextRecordMin);
                    nextRecordMax = KafkaIndexRecord.maxByValue(nextRecordMax, nextRecord);
                }

                if (nextRecordMax == RETRY_SEGMENT ||
                    nextRecordMax == NEXT_SEGMENT)
                {
                    nextRecord = nextRecordMax;
                    break;
                }
            }
            while (KafkaIndexRecord.value(nextRecordMin) != KafkaIndexRecord.value(nextRecordMax));

            return nextRecord;
        }

        @Override
        public boolean test(
            KafkaCacheEntryFW cacheEntry)
        {
            boolean accept = true;
            for (int i = 0; accept && i < conditions.size(); i++)
            {
                final KafkaCondition condition = conditions.get(i);
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

    public static final class Or extends KafkaCondition
    {
        private final List<KafkaCondition> conditions;

        public Or(
            List<KafkaCondition> conditions)
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
                position = KafkaIndexRecord.value(record);
            }

            long nextRecordMin = RETRY_SEGMENT;
            for (int i = 0; i < conditions.size(); i++)
            {
                final KafkaCondition condition = conditions.get(i);
                final long nextRecord = condition.reset(segment, offset, position);
                nextRecordMin = KafkaIndexRecord.minByValue(nextRecord, nextRecordMin);
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
                final KafkaCondition condition = conditions.get(i);
                final long nextRecord = condition.next(record);
                nextRecordMin = KafkaIndexRecord.minByValue(nextRecord, nextRecordMin);
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
                final KafkaCondition condition = conditions.get(i);
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
