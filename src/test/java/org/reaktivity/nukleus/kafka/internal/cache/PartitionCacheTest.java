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

    private PartitionCache cache = new PartitionCache(5, messageCache);

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
    public void shouldReturnEmptyIteratorIfEmpty()
    {
        Iterator<PartitionCache.Entry> iterator = cache.entries(0L);
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

    private static DirectBuffer asBuffer(String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        return new UnsafeBuffer(bytes);
    }

}
