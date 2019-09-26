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
package org.reaktivity.nukleus.kafka.internal.cache;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.kafka.internal.cache.MessageCache.NO_MESSAGE;
import static org.reaktivity.nukleus.kafka.internal.test.TestUtil.asBuffer;
import static org.reaktivity.nukleus.kafka.internal.test.TestUtil.asOctets;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;
import org.reaktivity.nukleus.kafka.internal.cache.CompactedPartitionIndex.EntryImpl;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex.Entry;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;
import org.reaktivity.nukleus.kafka.internal.types.codec.fetch.HeaderFW;

public final class CompactedPartitionIndexTest
{
    private static final int TOMBSTONE_LIFETIME_MILLIS = 5;

    private MessageCache messageCache;

    private DirectBuffer key = asBuffer("key");

    private DirectBuffer emptyHeadersBuffer = new UnsafeBuffer(new byte[0]);
    private HeadersFW emptyHeaders = new HeadersFW().wrap(emptyHeadersBuffer, 0, 0);

    private MessageFW anyMessage = new MessageFW.Builder().wrap(new UnsafeBuffer(new byte[100]), 0, 100)
            .timestamp(123L)
            .traceId(567L)
            .key(asOctets("key2"))
            .value(asOctets("value"))
            .build();

    private MessageFW matchMessage = new MessageFW.Builder().wrap(new UnsafeBuffer(new byte[100]), 0, 100)
            .timestamp(123L)
            .traceId(567L)
            .key(asOctets("key2"))
            .headers(builder -> builder.set(
                (b, offset, limit) -> new HeaderFW.Builder().wrap(b, offset, limit)
                    .keyLen("header1".length())
                    .key(asOctets("header1"))
                    .valueLen("match".length())
                    .value(asOctets("match"))
                    .build()
                    .sizeof()))
            .value(asOctets("value"))
            .build();
    private HeadersFW matchHeaders = new HeadersFW().wrap(matchMessage.headers());

    private MessageFW noMatchMessage = new MessageFW.Builder().wrap(new UnsafeBuffer(new byte[100]), 0, 100)
            .timestamp(123L)
            .traceId(567L)
            .key(asOctets("key1"))
            .headers(builder -> builder.set(
                (b, offset, limit) -> new HeaderFW.Builder().wrap(b, offset, limit)
                    .keyLen("header1".length())
                    .key(asOctets("header1"))
                    .valueLen("nomatch".length())
                    .value(asOctets("nomatch"))
                    .build()
                    .sizeof()))
            .value(asOctets("value"))
            .build();
    private HeadersFW noMatchHeaders = new HeadersFW().wrap(noMatchMessage.headers());

    private MutableDirectBuffer matchHeaderConditionBuffer = new UnsafeBuffer(new byte[100]);
    private ListFW<KafkaHeaderFW> matchHeaderCondition =
        new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW())
            .wrap(matchHeaderConditionBuffer, 0, matchHeaderConditionBuffer.capacity())
            .item(b -> b.key("header1").value(asOctets("match")))
            .build();

    private DirectBuffer value = asBuffer("value");

    private AtomicLong cacheHits = new AtomicLong();
    private AtomicLong cacheMisses = new AtomicLong();

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            messageCache = mock(MessageCache.class);
        }

        @Override
        public Statement apply(
            Statement base,
            FrameworkMethod method,
            Object target)
        {
            cacheHits.set(0L);
            cacheMisses.set(0L);
            return super.apply(base, method, target);
        }
    };

    private CompactedPartitionIndex index = new CompactedPartitionIndex(
                                                true,
                                                5,
                                                TOMBSTONE_LIFETIME_MILLIS,
                                                messageCache,
                                                cacheHits::incrementAndGet,
                                                cacheMisses::incrementAndGet);

    @Test
    public void shouldAddAndCacheNewMessage()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, key, emptyHeaders, value);
                will(returnValue(0));
            }
        });
        index.add(0L, 1L, 123, 456, key, emptyHeaders, value, true);
    }

    @Test
    public void shouldAddAndNotCacheNewMessage()
    {
        index.add(0L, 1L, 123, 456, key, emptyHeaders, value, false);
    }

    @Test
    public void shouldCompactWhenTooManyInvalidEntries()
    {
        long offset = 0;

        for (int i = 0; i <= CompactedPartitionIndex.MAX_INVALID_ENTRIES + 1; i++)
        {
            index.add(0L, offset++, 123L, 456L, key, emptyHeaders, value, false);
        }

        assertEquals(CompactedPartitionIndex.MAX_INVALID_ENTRIES + 2, index.numberOfEntries());

        index.add(0L, offset++, 123L, 456L, key, emptyHeaders, value, false);
        assertEquals(2, index.numberOfEntries());
    }


    @Test
    public void shouldAddTombstoneMessageAndReportUntilTombstoneExpires() throws Exception
    {
        final long future = currentTimeMillis() + 500L;

        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(future, 456, key, emptyHeaders, null);
                will(returnValue(0));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
                oneOf(messageCache).release(0);
            }
        });
        index.add(0L, 1L, future, 456, key, emptyHeaders, null, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, null);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(1L, entry.offset());
        Thread.sleep(future - currentTimeMillis() + TOMBSTONE_LIFETIME_MILLIS);
        iterator = index.entries(0L, null);
        assertEquals(2L, iterator.next().offset());
        assertEquals(1, cacheHits.get());
        assertEquals(0, cacheMisses.get());
    }

    @Test
    public void shouldAddTombstonesForExistingMessagesAndReportUntilExpired() throws Exception
    {
        final long aLongTime = 100 * TOMBSTONE_LIFETIME_MILLIS;
        final long timestamp1 = currentTimeMillis() + aLongTime;
        final long timestamp2 = timestamp1 + aLongTime;

        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(timestamp1, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(timestamp1, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
                oneOf(messageCache).replace(0, timestamp1, 458, asBuffer("key1"), emptyHeaders, null);
                will(returnValue(0));
                oneOf(messageCache).release(0);
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
                oneOf(messageCache).replace(1, timestamp2, 459, asBuffer("key2"), emptyHeaders, null);
                will(returnValue(1));
                oneOf(messageCache).release(1);
            }
        });
        index.add(0L, 0L, timestamp1, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(0L, 1L, timestamp1, 457, asBuffer("key2"), emptyHeaders, value, true);
        index.add(0L, 2L, timestamp1, 458, asBuffer("key1"), emptyHeaders, null, true);
        assert currentTimeMillis() < timestamp1 : "test failed due to unexpected execution delay";
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, null);
        Entry entry = iterator.next();
        assertEquals(1L, entry.offset());
        entry = iterator.next();
        assertEquals(2L, entry.offset());
        long delayTillAllEntriesAreExpired = timestamp1 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillAllEntriesAreExpired);
        index.add(2L, 3L, timestamp2, 459, asBuffer("key2"), emptyHeaders, null, true);
        assert currentTimeMillis() < timestamp2 : "test failed due to unexpected execution delay";
        iterator = index.entries(0L, null);
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        entry = iterator.next();
        assertEquals(4L, entry.offset());
        assertEquals(NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
        delayTillAllEntriesAreExpired = timestamp2 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillAllEntriesAreExpired);
        iterator = index.entries(0L, null);
        assertEquals(4L, iterator.next().offset());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldAddTombstonesForExistingMessagesAndReportUntilExpiredWhenNotCachingMessages() throws Exception
    {
        final long aLongTime = 100 * TOMBSTONE_LIFETIME_MILLIS;
        final long timestamp1 = currentTimeMillis() + aLongTime;
        final long timestamp2 = timestamp1 + aLongTime;

        context.checking(new Expectations()
        {
            {
                allowing(messageCache).get(with(NO_MESSAGE), with(any(MessageFW.class)));
                will(returnValue(null));
            }
        });

        index.add(0L, 0L, timestamp1, 456, asBuffer("key1"), emptyHeaders, value, false);
        index.add(0L, 1L, timestamp1, 457, asBuffer("key2"), emptyHeaders, value, false);
        index.add(0L, 2L, timestamp1, 458, asBuffer("key1"), emptyHeaders, null, false);
        assert currentTimeMillis() < timestamp1 : "test failed due to unexpected execution delay";
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, null);
        Entry entry = iterator.next();
        assertEquals(1L, entry.offset());
        entry = iterator.next();
        assertEquals(2L, entry.offset());
        long delayTillAllEntriesAreExpired = timestamp1 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillAllEntriesAreExpired);
        index.add(2L, 3L, timestamp2, 459, asBuffer("key2"), emptyHeaders, null, false);
        assert currentTimeMillis() < timestamp2 : "test failed due to unexpected execution delay";
        iterator = index.entries(0L, null);
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        assertFalse(iterator.hasNext());
        delayTillAllEntriesAreExpired = timestamp2 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillAllEntriesAreExpired);
        iterator = index.entries(0L, null);
        assertEquals(4L, iterator.next().offset());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReplaceTombstoneWithNewMessageAndRemoveOnlyWhenASubsequentTombstoneExpires() throws Exception
    {
        final long aLongTime = 100 * TOMBSTONE_LIFETIME_MILLIS;
        final long timestamp1 = currentTimeMillis() + aLongTime;
        final long timestamp2 = timestamp1 + aLongTime;

        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(timestamp1, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
                oneOf(messageCache).replace(0, timestamp1, 457, asBuffer("key1"), emptyHeaders, null);
                will(returnValue(0));
                oneOf(messageCache).replace(0, timestamp1, 458, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).replace(0, timestamp2, 459, asBuffer("key1"), emptyHeaders, null);
                will(returnValue(0));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
                oneOf(messageCache).release(0);
            }
        });
        index.add(0L, 0L, timestamp1, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(1L, 1L, timestamp1, 457, asBuffer("key1"), emptyHeaders, null, true);
        index.add(2L, 2L, timestamp1, 458, asBuffer("key1"), emptyHeaders, value, true);
        assert currentTimeMillis() < timestamp1 : "test failed due to unexpected execution delay";
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, null);
        Entry entry = iterator.next();
        assertEquals(2L, entry.offset());
        assertEquals(0, entry.messageHandle());
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        assertEquals(NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
        long delayTillEntryHasExpired = timestamp1 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillEntryHasExpired);
        iterator = index.entries(0L, null);
        entry = iterator.next();

        // Since the message now has a value again it should not be remove by expiry of the previous tombstone
        assertEquals(0, entry.messageHandle());

        assertEquals(2L, entry.offset());
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        assertEquals(NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());

        index.add(3L, 3L, timestamp2, 459, asBuffer("key1"), emptyHeaders, null, true);
        delayTillEntryHasExpired = timestamp2 + TOMBSTONE_LIFETIME_MILLIS - currentTimeMillis();
        Thread.sleep(delayTillEntryHasExpired);
        iterator = index.entries(0L, null);
        entry = iterator.next();
        assertEquals(4L, entry.offset());
        assertEquals(NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotAddToEntriesIteratorMessagesWithOffsetsOutOfOrder()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(anyMessage));

            }
        });
        index.add(101L, 110L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(0L, 100L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        index.add(102L, 110L, 125, 458, asBuffer("key3"), emptyHeaders, value, true);
        index.add(101L, 101L, 126, 459, asBuffer("key4"), emptyHeaders, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(100L, null);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(110L, entry.offset());
        assertEquals(0, entry.messageHandle());
        assertTrue(iterator.hasNext());
        entry = iterator.next();
        assertEquals(111L, entry.offset());
        assertEquals(NO_MESSAGE, entry.messageHandle());
    }

    @Test
    public void shouldGetEntriesForExistingKeys()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(null));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        Entry entry = index.getEntry(asOctets("key1"));
        assertEquals(0L, entry.offset());
        assertEquals(0, entry.messageHandle());
        entry = index.getEntry(asOctets("key2"));
        assertEquals(1L, entry.offset());
        assertEquals(1, entry.messageHandle());
        assertEquals(1, cacheHits.get());
        assertEquals(1, cacheMisses.get());
    }

    @Test
    public void shouldGetOffsetsForExistingKeysWithoutIncrementingCacheHitsMisses()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        long offset = index.getOffset(asOctets("key1"));
        assertEquals(0L, offset);
        offset = index.getOffset(asOctets("key2"));
        assertEquals(1L, offset);

        assertEquals(0, cacheHits.get());
        assertEquals(0, cacheMisses.get());
    }

    @Test
    public void shouldReportNullFromGetEntryForNonExistingKey()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 1L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(0L, 2L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        Entry entry = index.getEntry(asOctets("unknownKey"));
        assertNull(entry);
    }

    @Test
    public void shouldIterateOverAllKeys()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), matchHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(matchMessage));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), matchHeaders, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, null);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(0L, entry.offset());
        assertEquals(0, entry.messageHandle());
        entry = iterator.next();
        assertEquals(1L, entry.offset());
        assertEquals(1, entry.messageHandle());

        // Final entry should report end of series (no message) and the latest known offset
        entry = iterator.next();
        assertEquals(2L, entry.offset());
        assertEquals(NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldIterateOverAllKeysApplyingHeaderConditions()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), noMatchHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), matchHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(noMatchMessage));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(matchMessage));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), noMatchHeaders, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), matchHeaders, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, matchHeaderCondition);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(1L, entry.offset());
        assertEquals(1, entry.messageHandle());
        assertTrue(iterator.hasNext());
        entry = iterator.next();
        assertEquals(2L, entry.offset());
        assertEquals(NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldBehaveSensiblyWhenNoMoreElements()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
            }
        });
        index.add(0L, 1L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, null);
        assertTrue(iterator.hasNext());
        assertNotNull(iterator.next());
        assertNotNull(iterator.next());
        assertFalse(iterator.hasNext());
        iterator.next(); // this now just silently returns the terminating element with NO_MESSAGE
    }

    @Test
    public void shouldIterateFromNextHighestOffsetWhenRequestedOffsetNotFound()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).put(125, 458, asBuffer("key3"), emptyHeaders, value);
                will(returnValue(2));
                oneOf(messageCache).replace(1, 126, 459, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(3));
                oneOf(messageCache).get(with(3), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        index.add(0L, 2L, 125, 458, asBuffer("key3"), emptyHeaders, value, true);
        index.add(0L, 10L, 126, 459, asBuffer("key2"), emptyHeaders, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(5L, null);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(10L, entry.offset());
        assertEquals(3, entry.messageHandle());
    }

    @Test
    public void shouldReturnIteratorWithRequestedOffsetAndNoMessageWhenCacheIsEmpty()
    {
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(100L, null);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(100L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void shouldThrowExceptionFromNoMessageIteratorNextWhenNoMoreElements()
    {
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(102L, null);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(102L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
        iterator.next();
    }

    @Test
    public void shouldReturnIteratorWithRequestedOffsetAndNoMessageWhenRequestOffsetExceedsHighestOffsetSeenSoFar()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
            }
        });
        index.add(0L, 101L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(102L, null);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(102L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldCacheHistoricalMessageWhenItHasBeenEvicted()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).put(125, 458, asBuffer("key3"), emptyHeaders, value);
                will(returnValue(2));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(null));
                oneOf(messageCache).replace(1, 124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 101L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(100L, 102L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        index.add(100L, 103L, 125, 458, asBuffer("key3"), emptyHeaders, value, true);
        index.add(100L, 102L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
    }

    @Test
    public void shouldNotCacheHistoricalMessageWhenItHasNotBeenEvicted()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).put(125, 458, asBuffer("key3"), emptyHeaders, value);
                will(returnValue(2));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
            }
        });
        index.add(0L, 101L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(100L, 102L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        index.add(100L, 103L, 125, 458, asBuffer("key3"), emptyHeaders, value, true);
        index.add(100L, 102L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
    }

    @Test
    public void shouldNotUpdateEntryForExistingKeyWhenAddHasRequestOffsetExceedsHighestOffsetSeenSoFar()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
            }
        });
        index.add(0L, 110L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(112L, 113L, 124, 457, asBuffer("key1"), emptyHeaders, value, true);
    }

    @Test
    public void shouldAddEntryForKeyButNotIncreaseHighwaterMarkWhenRequestOffsetExceedsHighestOffsetSeenSoFar()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
            }
        });
        index.add(0L, 110L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(112L, 113L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
    }

    @Test
    public void shouldReplaceOffsetsForKeys()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).replace(1, 125, 458, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).replace(0, 126, 459, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).get(with(0), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
            }
        });
        index.add(0L, 0L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);
        index.add(0L, 2L, 125, 458, asBuffer("key2"), emptyHeaders, value, true);
        index.add(0L, 3L, 126, 459, asBuffer("key1"), emptyHeaders, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, null);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(2L, entry.offset());
        assertEquals(1, entry.messageHandle());
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        assertEquals(0, entry.messageHandle());
        entry = iterator.next();
        assertEquals(4L, entry.offset());
        assertEquals(NO_MESSAGE, entry.messageHandle());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReportEntryAsString()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(77));
                oneOf(messageCache).get(with(77), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
            }
        });
        index.add(0L, 100L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        Iterator<CompactedPartitionIndex.Entry> iterator = index.entries(0L, null);
        Entry entry = iterator.next();
        String result = entry.toString();
        assertTrue(result.contains("100"));
        assertTrue(result.contains("77"));
    }

    @Test
    public void shouldMaintainIsTombstoneStateOnEntryImpl()
    {
        EntryImpl entry = new EntryImpl(123L, NO_MESSAGE, 0);
        assertFalse(entry.isTombstone());
        assertEquals(0, entry.position());

        assertFalse(entry.getAndSetIsTombstone(true));
        assertTrue(entry.isTombstone());
        assertEquals(0, entry.position());

        assertTrue(entry.getAndSetIsTombstone(false));
        assertFalse(entry.isTombstone());
        assertFalse(entry.getAndSetIsTombstone(false));
        assertFalse(entry.isTombstone());
        assertEquals(0, entry.position());

        entry.setPosition(1);
        assertFalse(entry.isTombstone());
        assertFalse(entry.getAndSetIsTombstone(true));
        assertTrue(entry.isTombstone());
        assertEquals(1, entry.position());
        entry.setPosition(2);
        assertTrue(entry.isTombstone());
    }


    @Test
    public void shouldEvictMessagesBeforeStartOffset()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), emptyHeaders, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), emptyHeaders, value);
                will(returnValue(1));
                oneOf(messageCache).release(0);
                oneOf(messageCache).get(with(1), with(any(MessageFW.class)));
                will(returnValue(anyMessage));
            }
        });

        index.add(0L, 0L, 123, 456, asBuffer("key1"), emptyHeaders, value, true);
        index.add(0L, 1L, 124, 457, asBuffer("key2"), emptyHeaders, value, true);

        index.startOffset(1L);

        Entry entry1 = index.getEntry(asOctets("key1"));
        Entry entry2 = index.getEntry(asOctets("key2"));

        assertNull(entry1);

        assertEquals(1L, entry2.offset());
        assertEquals(1, entry2.messageHandle());
    }

}
