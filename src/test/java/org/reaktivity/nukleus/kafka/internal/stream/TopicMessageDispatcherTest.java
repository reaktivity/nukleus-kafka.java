/**
 * Copyright 2016-2018 The Reaktivity Project
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.kafka.internal.stream.MessageDispatcher.FLAGS_DELIVERED;
import static org.reaktivity.nukleus.kafka.internal.test.TestUtil.asBuffer;
import static org.reaktivity.nukleus.kafka.internal.test.TestUtil.asOctets;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.cache.TopicCache;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;

public final class TopicMessageDispatcherTest
{
    private Iterator<KafkaHeaderFW> emptyHeaders = Collections.emptyIterator();

    private final ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW> headersRW =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW());
       private final MutableDirectBuffer headersBuffer = new UnsafeBuffer(new byte[1000]);

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    private TopicCache topicCache = context.mock(TopicCache.class, "topicCache");

    private TopicMessageDispatcher dispatcher =
            new TopicMessageDispatcher(topicCache, 3, false, HeaderValueMessageDispatcher::new);

    @Test
    public void shouldAddDispatcherWithEmptyHeadersAndNullKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        dispatcher.add(null, -1, emptyHeaders, child1);
    }

    @Test
    public void shouldAddDispatcherWithNullHeadersAndNullKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        dispatcher.add(null, -1, emptyHeaders, child1);
    }

    @Test
    public void shouldAddMultipleDispatchersWithSameKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), 0, emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), 0, emptyHeaders, child2);
    }

    @Test
    public void shouldAdjustOffset()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child2);
        context.checking(new Expectations()
        {
            {
                oneOf(child1).adjustOffset(1, 10L, 5L);
                oneOf(child2).adjustOffset(1, 10L, 5L);
            }
        });
        dispatcher.adjustOffset(1, 10L, 5L);
    }

    @Test
    public void shouldDetach()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child2);
        context.checking(new Expectations()
        {
            {
                oneOf(child1).detach(true);
                oneOf(child2).detach(true);
            }
        });
        dispatcher.detach(true);
    }

    @Test
    public void shouldDispatchWithoutKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(null, -1, emptyHeaders, child1);
        dispatcher.add(null, -1, emptyHeaders, child2);

        HeadersFW headers = emptyHeaders();

        final long timestamp = System.currentTimeMillis() - 123;
        final long traceId = 0L;

        context.checking(new Expectations()
        {
            {
                oneOf(child1).dispatch(with(1), with(10L), with(12L), with((DirectBuffer) null),
                        with(headers.headerSupplier()), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
                oneOf(child2).dispatch(with(1), with(10L), with(12L), with((DirectBuffer) null),
                        with(headers.headerSupplier()), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
                oneOf(topicCache).add(with(1), with(10L), with(12L), with(timestamp), with(traceId),
                        with((DirectBuffer) null), with(headers), with((DirectBuffer) null), with(false));
            }
        });
        assertEquals(FLAGS_DELIVERED, dispatcher.dispatch(1, 10L, 12L, 999L, null, headers, timestamp, traceId, null));
    }


    @Test
    public void shouldDispatchWithMatchingKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher child3 = context.mock(MessageDispatcher.class, "child3");
        dispatcher.add(asOctets("key1"), 0, emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), 0, emptyHeaders, child2);
        dispatcher.add(asOctets("key2"), 1, emptyHeaders, child3);

        HeadersFW headers = emptyHeaders();

        final long timestamp1 = System.currentTimeMillis() - 123;
        final long timestamp2 = System.currentTimeMillis() - 123;
        final long traceId = 0L;

        context.checking(new Expectations()
        {
            {
                oneOf(child1).dispatch(with(0), with(10L), with(12L), with(bufferMatching("key1")),
                        with(headers.headerSupplier()), with(timestamp1), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
                oneOf(child2).dispatch(with(0), with(10L), with(12L), with(bufferMatching("key1")),
                        with(headers.headerSupplier()), with(timestamp1), with(traceId), with((DirectBuffer) null));
                oneOf(topicCache).getOffset(with(0), with(asOctets("key1")));
                oneOf(topicCache).nextOffset(0);
                will(returnValue(0L));
                oneOf(topicCache).add(with(0), with(10L), with(12L), with(timestamp1), with(traceId),
                        with(bufferMatching("key1")), with(headers), with((DirectBuffer) null), with(false));

                oneOf(child3).dispatch(with(1), with(10L), with(13L), with(bufferMatching("key2")),
                        with(headers.headerSupplier()), with(timestamp2), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
                oneOf(topicCache).getOffset(with(1), with(asOctets("key2")));
                oneOf(topicCache).nextOffset(1);
                will(returnValue(0L));
                oneOf(topicCache).add(with(1), with(10L), with(13L), with(timestamp1), with(traceId),
                        with(bufferMatching("key2")), with(headers), with((DirectBuffer) null), with(false));
            }
        });
        assertEquals(FLAGS_DELIVERED,
                dispatcher.dispatch(0, 10L, 12L, 999L, asBuffer("key1"), headers, timestamp1, traceId, null));
        assertEquals(FLAGS_DELIVERED,
                dispatcher.dispatch(1, 10L, 13L, 999L, asBuffer("key2"), headers, timestamp2, traceId, null));
    }

    @Test
    public void shouldCacheOffsetButNotDispatchNonMatchingKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher routeMatcher = context.mock(MessageDispatcher.class, "routeMatcher");
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child2);
        dispatcher.add(null, 1, emptyHeaders, routeMatcher);

        HeadersFW headers = emptyHeaders();

        final long timestamp = System.currentTimeMillis() - 123;

        context.checking(new Expectations()
        {
            {
                oneOf(topicCache).getOffset(with(1), with(asOctets("key2")));
                oneOf(topicCache).nextOffset(with(1));
                will(returnValue(0L));
                oneOf(routeMatcher).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key2")),
                        with(headers.headerSupplier()), with(timestamp), with(0L), with((DirectBuffer) null));
                will(returnValue(MessageDispatcher.FLAGS_MATCHED));
                oneOf(topicCache).add(with(1), with(10L), with(12L), with(timestamp), with(0L),
                        with(bufferMatching("key2")), with(headers), with((DirectBuffer) null), with(false));
            }
        });
        assertEquals(MessageDispatcher.FLAGS_MATCHED,
                dispatcher.dispatch(1, 10L, 12L, 999L, asBuffer("key2"), headers, timestamp, 0L, null));
    }

    @Test
    public void shouldCacheOffsetAndMessageWhenProactiveMessageCachingEnabled()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher routeMatcher = context.mock(MessageDispatcher.class, "routeMatcher");
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child2);
        dispatcher.add(null, 1, emptyHeaders, routeMatcher);

        HeadersFW headers = emptyHeaders();

        final long timestamp = System.currentTimeMillis() - 123;

        context.checking(new Expectations()
        {
            {
                oneOf(topicCache).getOffset(with(1), with(asOctets("key2")));
                oneOf(topicCache).nextOffset(with(1));
                will(returnValue(0L));
                oneOf(routeMatcher).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key2")),
                        with(headers.headerSupplier()), with(timestamp), with(0L), with((DirectBuffer) null));
                will(returnValue(MessageDispatcher.FLAGS_MATCHED));
                oneOf(topicCache).add(with(1), with(10L), with(12L), with(timestamp), with(0L),
                        with(bufferMatching("key2")), with(headers), with((DirectBuffer) null), with(true));
            }
        });
        dispatcher.enableProactiveMessageCaching();
        assertEquals(MessageDispatcher.FLAGS_MATCHED,
                dispatcher.dispatch(1, 10L, 12L, 999L, asBuffer("key2"), headers, timestamp, 0L, null));
    }

    @Test
    public void shouldCacheOffsetAndMessageWhenLCaughtUpToLiveStream()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher routeMatcher = context.mock(MessageDispatcher.class, "routeMatcher");
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child2);
        dispatcher.add(null, 1, emptyHeaders, routeMatcher);

        HeadersFW headers = emptyHeaders();

        final long timestamp = System.currentTimeMillis() - 123;

        context.checking(new Expectations()
        {
            {
                oneOf(topicCache).getOffset(with(1), with(asOctets("key2")));
                oneOf(topicCache).nextOffset(with(1));
                will(returnValue(0L));
                oneOf(routeMatcher).dispatch(with(1), with(10L), with(11L), with(bufferMatching("key2")),
                        with(headers.headerSupplier()), with(timestamp), with(0L), with((DirectBuffer) null));
                will(returnValue(MessageDispatcher.FLAGS_MATCHED));
                oneOf(topicCache).add(with(1), with(10L), with(11L), with(timestamp), with(0L),
                        with(bufferMatching("key2")), with(headers), with((DirectBuffer) null), with(true));
            }
        });
        assertEquals(MessageDispatcher.FLAGS_MATCHED,
                dispatcher.dispatch(1, 10L, 11L, 12L, asBuffer("key2"), headers, timestamp, 0L, null));
    }

    @Test
    public void shouldFlush()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child1);
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child2);
        context.checking(new Expectations()
        {
            {
                oneOf(child1).flush(1, 10L, 12L);
                oneOf(child2).flush(1, 10L, 12L);
                oneOf(topicCache).extendNextOffset(1, 10L, 12L);
            }
        });
        dispatcher.flush(1, 10L, 12L);
    }

    @Test
    public void shouldRemoveKeyDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), 1, emptyHeaders, child1);
        dispatcher.add(asOctets("key2"), 0, emptyHeaders, child2);
        assertTrue(dispatcher.remove(asOctets("key1"), 1, emptyHeaders, child1));
        assertTrue(dispatcher.remove(asOctets("key2"), 0, emptyHeaders, child2));
        assertTrue(dispatcher.isEmpty());
    }

    @Test
    public void shouldRemoveHeadersDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers = new ArrayList<>();
        headersRW.wrap(headersBuffer, 0, headersBuffer.capacity())
            .item(b -> b.key("header1").value(asOctets("value1")))
            .build()
            .forEach(h -> headers.add(h));
        dispatcher.add(null, -1, headers.iterator(), child1);
        dispatcher.add(asOctets("key1"), 1, headers.iterator(), child2);
        assertTrue(dispatcher.remove(null, -1, headers.iterator(), child1));
        assertTrue(dispatcher.remove(asOctets("key1"), 1, headers.iterator(), child2));
        assertTrue(dispatcher.isEmpty());
    }

    @Test
    public void shouldRemoveUnconditionalDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(null, -1, emptyHeaders, child1);
        dispatcher.add(null, -1, emptyHeaders, child2);
        assertTrue(dispatcher.remove(null, -1, emptyHeaders, child1));
        assertTrue(dispatcher.remove(null, -1, emptyHeaders, child2));
        assertTrue(dispatcher.isEmpty());
    }

    @Test
    public void shouldNotRemoveDispatcherWhenNotPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");

        assertFalse(dispatcher.remove(asOctets("key1"), 0, null, child1));

        dispatcher.add(asOctets("key1"), 0, emptyHeaders, child1);
        assertFalse(dispatcher.remove(asOctets("key1"), 0, emptyHeaders, child2));
    }

    private Matcher<DirectBuffer> bufferMatching(final String string)
    {
        return new BaseMatcher<DirectBuffer>()
        {

            @Override
            public boolean matches(Object item)
            {
                return item instanceof UnsafeBuffer &&
                        ((UnsafeBuffer)item).equals(new UnsafeBuffer(string.getBytes(UTF_8)));
            }

            @Override
            public void describeTo(Description description)
            {
                description.appendText(string);
            }

        };
    }

    private HeadersFW emptyHeaders()
    {
        HeadersFW result = new HeadersFW();
        DirectBuffer buffer = new UnsafeBuffer(new byte[] { (byte) 0});
        result.wrap(buffer, 0, 1);
        return result;
    }

}
