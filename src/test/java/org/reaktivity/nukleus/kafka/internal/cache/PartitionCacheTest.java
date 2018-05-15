/**
 * Copyright 2016-2017 The Reaktivity Project
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionCache.Entry;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;

public final class PartitionCacheTest
{
    private static final int TOMBSTONE_LIFETIME_MILLIS = 1;

    private MessageCache messageCache;

    private DirectBuffer key = asBuffer("key");
    private DirectBuffer headersBuffer = new UnsafeBuffer(new byte[0]);
    private HeadersFW headers = new HeadersFW().wrap(headersBuffer, 0, 0);
    private DirectBuffer value = asBuffer("value");

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            messageCache = mock(MessageCache.class);
        }
    };

    private PartitionCache cache = new PartitionCache(5, TOMBSTONE_LIFETIME_MILLIS, messageCache);

    @Test
    public void shouldAddMessage()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, key, headers, value);
                will(returnValue(0));
            }
        });
        cache.add(0L, 1L, 123, 456, key, headers, value);
    }

    @Test
    public void shouldAddTombstoneMessageAndReportUntilTombstoneExpires() throws Exception
    {
        cache.add(0L, 2L, 123, 456, key, headers, null);
        Iterator<PartitionCache.Entry> iterator = cache.entries(0L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(1L, entry.offset());
        assertEquals(PartitionCache.TOMBSTONE_MESSAGE, entry.message());
        Thread.sleep(TOMBSTONE_LIFETIME_MILLIS);
        iterator = cache.entries(0L);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldNotAddMessagesWithOffsetsOutOfOrder()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(126, 459, asBuffer("key4"), headers, value);
                will(returnValue(1));
            }
        });
        cache.setPartitionStartOffset(100L);
        cache.add(101L, 111L, 123, 456, asBuffer("key1"), headers, value);
        cache.add(100L, 101L, 124, 457, asBuffer("key2"), headers, value);
        cache.add(102L, 111L, 125, 458, asBuffer("key3"), headers, value);
        cache.add(101L, 102L, 126, 459, asBuffer("key4"), headers, value);
        Iterator<PartitionCache.Entry> iterator = cache.entries(100L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(100L, entry.offset());
        assertEquals(0, entry.message());
        entry = iterator.next();
        assertEquals(101L, entry.offset());
        assertEquals(1, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldIterate()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
            }
        });
        cache.add(0L, 1L, 123, 456, asBuffer("key1"), headers, value);
        cache.add(0L, 2L, 124, 457, asBuffer("key2"), headers, value);
        Iterator<PartitionCache.Entry> iterator = cache.entries(0L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(0L, entry.offset());
        assertEquals(0, entry.message());
        entry = iterator.next();
        assertEquals(1L, entry.offset());
        assertEquals(1, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldIterateFromNextHighestOffsetWhenRequestedOffsetNotFound()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).put(125, 458, asBuffer("key3"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).replace(1, 126, 459, asBuffer("key2"), headers, value);
                will(returnValue(1));
            }
        });
        cache.add(0L, 1L, 123, 456, asBuffer("key1"), headers, value);
        cache.add(0L, 2L, 124, 457, asBuffer("key2"), headers, value);
        cache.add(0L, 3L, 125, 458, asBuffer("key3"), headers, value);
        cache.add(0L, 11L, 126, 459, asBuffer("key2"), headers, value);
        Iterator<PartitionCache.Entry> iterator = cache.entries(5L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(10L, entry.offset());
        assertEquals(1, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReturnIteratorWithRequestedOffsetAndNoMessageWhenCacheIsEmpty()
    {
        Iterator<PartitionCache.Entry> iterator = cache.entries(100L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(100L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReturnIteratorWithRequestedOffsetAndNoMessageWhenRequestOffsetExceedsHighestOffsetSeenSoFar()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
            }
        });
        cache.add(0L, 101L, 123, 456, asBuffer("key1"), headers, value);
        Iterator<PartitionCache.Entry> iterator = cache.entries(102L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(102L, entry.offset());
        assertEquals(MessageCache.NO_MESSAGE, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldCacheHistoricalMessageWhenItHasBeenEvicted()
    {
         throw new RuntimeException("Test not yet implemented");
    }

    @Test
    public void shouldNotCacheHistoricalMessageWhenItHasNotBeenEvicted()
    {
         throw new RuntimeException("Test not yet implemented");
    }

    @Test
    public void shouldReplaceOffsetsForKeys()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(0));
                oneOf(messageCache).put(124, 457, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).replace(1, 125, 458, asBuffer("key2"), headers, value);
                will(returnValue(1));
                oneOf(messageCache).replace(0, 126, 459, asBuffer("key1"), headers, value);
                will(returnValue(0));
            }
        });
        cache.add(0L, 1L, 123, 456, asBuffer("key1"), headers, value);
        cache.add(0L, 2L, 124, 457, asBuffer("key2"), headers, value);
        cache.add(0L, 3L, 125, 458, asBuffer("key2"), headers, value);
        cache.add(0L, 4L, 126, 459, asBuffer("key1"), headers, value);
        Iterator<PartitionCache.Entry> iterator = cache.entries(0L);
        assertTrue(iterator.hasNext());
        Entry entry = iterator.next();
        assertEquals(2L, entry.offset());
        assertEquals(1, entry.message());
        entry = iterator.next();
        assertEquals(3L, entry.offset());
        assertEquals(0, entry.message());
        assertFalse(iterator.hasNext());
    }

    @Test
    public void shouldReportEntryAsString()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(messageCache).put(123, 456, asBuffer("key1"), headers, value);
                will(returnValue(77));
            }
        });
        cache.add(0L, 100L, 123, 456, asBuffer("key1"), headers, value);
        Iterator<PartitionCache.Entry> iterator = cache.entries(0L);
        Entry entry = iterator.next();
        String result = entry.toString();
        assertTrue(result.contains("99"));
        assertTrue(result.contains("77"));
    }

    private static DirectBuffer asBuffer(String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        return new UnsafeBuffer(bytes);
    }

}
