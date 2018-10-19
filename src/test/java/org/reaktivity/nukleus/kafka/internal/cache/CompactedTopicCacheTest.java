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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.kafka.internal.cache.TopicCache.NO_MESSAGE;
import static org.reaktivity.nukleus.kafka.internal.cache.TopicCache.NO_OFFSET;
import static org.reaktivity.nukleus.kafka.internal.test.TestUtil.asBuffer;

import java.util.Iterator;

import org.agrona.DirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.jmock.Expectations;
import org.jmock.Sequence;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.cache.ImmutableTopicCache.Message;
import org.reaktivity.nukleus.kafka.internal.cache.PartitionIndex.Entry;
import org.reaktivity.nukleus.kafka.internal.stream.HeadersFW;
import org.reaktivity.nukleus.kafka.internal.types.MessageFW;

public final class CompactedTopicCacheTest
{
    private MessageCache messageCache;
    private PartitionIndex index0;
    private PartitionIndex index1;
    private PartitionIndex index2;
    private Iterator<PartitionIndex.Entry> iterator0;
    private Iterator<PartitionIndex.Entry> iterator1;
    private Iterator<PartitionIndex.Entry> iterator2;
    private Entry entry0;
    private Entry entry1;
    private Entry entry2;

    private DirectBuffer key = asBuffer("key");
    private DirectBuffer headersBuffer = new UnsafeBuffer(new byte[0]);
    private HeadersFW headers = new HeadersFW().wrap(headersBuffer, 0, 0);
    private DirectBuffer value = asBuffer("value");
    private MessageFW messageRO = new MessageFW();


    @SuppressWarnings("unchecked")
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery()
    {
        {
            messageCache = mock(MessageCache.class);
            index0 = mock(PartitionIndex.class, "index0");
            index1 = mock(PartitionIndex.class, "index1");
            index2 = mock(PartitionIndex.class, "index2");
            iterator0 = mock(Iterator.class, "iterator0");
            iterator1 = mock(Iterator.class, "iterator1");
            iterator2 = mock(Iterator.class, "iterator2");
            entry0 = mock(Entry.class, "entry0");
            entry1= mock(Entry.class, "entry1");
            entry2 = mock(Entry.class, "entry2");
        }
    };

    private Sequence order = context.sequence("order");

    private CompactedTopicCache cache = new CompactedTopicCache(
            new PartitionIndex[]{index0, index1, index2}, messageCache);

    @Test
    public void shouldAddAndCacheNewMessage()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(index0).add(0L, 1L, 123, 456, key, headers, value, true);
                oneOf(index2).add(1L, 10L, 234, 456, key, headers, value, true);
                oneOf(index1).add(2L, 3L, 345, 456, key, headers, value, true);
            }
        });
        cache.add(0, 0L, 1L, 123, 456, key, headers, value, true);
        cache.add(2, 1L, 10L, 234, 456, key, headers, value, true);
        cache.add(1, 2L, 3L, 345, 456, key, headers, value, true);
    }

    @Test
    public void shouldGetMessagesFromAlternatingPartitions()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(index0).entries(10L, null); will(returnValue(iterator0));
                oneOf(index1).entries(11L, null); will(returnValue(iterator1));
                oneOf(index2).entries(12L, null); will(returnValue(iterator2));

                oneOf(iterator0).hasNext(); will(returnValue(true));
                oneOf(iterator0).next(); will(returnValue(entry0));
                oneOf(entry0).offset();
                oneOf(entry0).messageHandle(); will(returnValue(111));
                oneOf(messageCache).get(with(111), with(any(MessageFW.class)));
                will(returnValue(messageRO));

                oneOf(iterator1).hasNext(); will(returnValue(true));
                oneOf(iterator1).next(); will(returnValue(entry1));
                oneOf(entry1).offset();
                oneOf(entry1).messageHandle(); will(returnValue(NO_MESSAGE));

                oneOf(iterator2).hasNext(); will(returnValue(true));
                oneOf(iterator2).next(); will(returnValue(entry2));
                oneOf(entry2).offset();
                oneOf(entry2).messageHandle(); will(returnValue(NO_MESSAGE));

                oneOf(iterator0).hasNext(); will(returnValue(true));
                oneOf(iterator0).next(); will(returnValue(entry0));
                oneOf(entry0).offset();
                oneOf(entry0).messageHandle(); will(returnValue(NO_MESSAGE));

                oneOf(iterator1).hasNext(); will(returnValue(false)); inSequence(order);
                oneOf(iterator2).hasNext(); will(returnValue(false)); inSequence(order);
                oneOf(iterator0).hasNext(); will(returnValue(false)); inSequence(order);
            }
        });
        Long2LongHashMap fetchOffsets = new Long2LongHashMap(NO_OFFSET);
        fetchOffsets.put(0,  10L);
        fetchOffsets.put(1,  11L);
        fetchOffsets.put(2,  12L);

        Iterator<Message> messages = cache.getMessages(fetchOffsets, null, null);

        // First message is found in cache from partition 0
        assertTrue(messages.hasNext());
        Message message = messages.next();
        assertSame(messageRO, message.message());

        // cycle through partitions 1,2 and back to 0, no more messages are available in cache
        for (int i=0; i < 3; i++)
        {
            assertTrue(messages.hasNext());
            message = messages.next();
            assertNotNull(message);
        }
        assertFalse(messages.hasNext());
    }

    @Test
    public void shouldRotateStartingPartitionForHasNextForFairness()
    {
        context.checking(new Expectations()
        {
            {
                exactly(4).of(index0).entries(10L, null);
                will(returnValue(iterator0));
                exactly(4).of(index1).entries(11L, null);
                will(returnValue(iterator1));
                exactly(4).of(index2).entries(12L, null);
                will(returnValue(iterator2));

                oneOf(iterator0).hasNext(); will(returnValue(true)); inSequence(order);
                oneOf(iterator0).next(); will(returnValue(entry0)); inSequence(order);
                oneOf(entry0).offset();
                oneOf(entry0).messageHandle(); will(returnValue(NO_MESSAGE));

                oneOf(iterator1).hasNext(); will(returnValue(true)); inSequence(order);
                oneOf(iterator1).next(); will(returnValue(entry1)); inSequence(order);
                oneOf(entry1).offset();
                oneOf(entry1).messageHandle(); will(returnValue(NO_MESSAGE));

                oneOf(iterator2).hasNext(); will(returnValue(true)); inSequence(order);
                oneOf(iterator2).next(); will(returnValue(entry2)); inSequence(order);
                oneOf(entry2).offset();
                oneOf(entry2).messageHandle(); will(returnValue(NO_MESSAGE));

                oneOf(iterator0).hasNext(); will(returnValue(true)); inSequence(order);
                oneOf(iterator0).next(); will(returnValue(entry0)); inSequence(order);
                oneOf(entry0).offset();
                oneOf(entry0).messageHandle(); will(returnValue(NO_MESSAGE));
            }
        });

        Long2LongHashMap fetchOffsets = new Long2LongHashMap(NO_OFFSET);
        fetchOffsets.put(0,  10L);
        fetchOffsets.put(1,  11L);
        fetchOffsets.put(2,  12L);

        Iterator<Message> messages;

        for (int i=0; i < 4; i++)
        {
            messages = cache.getMessages(fetchOffsets, null, null);
            assertTrue(messages.hasNext());
            messages.next();
        }
    }

}
