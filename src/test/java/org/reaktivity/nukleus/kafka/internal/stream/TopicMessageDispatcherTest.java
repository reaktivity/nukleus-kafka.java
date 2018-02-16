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
package org.reaktivity.nukleus.kafka.internal.stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.function.Function;

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
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;
import org.reaktivity.nukleus.kafka.internal.types.stream.KafkaHeaderFW;

public final class TopicMessageDispatcherTest
{
    private TopicMessageDispatcher dispatcher = new TopicMessageDispatcher();

    private final ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW> headersRW =
            new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW());
       private final MutableDirectBuffer headersBuffer = new UnsafeBuffer(new byte[1000]);

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldAddDispatcherWithEmptyHeadersAndNullKey()
    {
        ListFW<KafkaHeaderFW> headers = headersRW.wrap(headersBuffer, 0, headersBuffer.capacity())
                                                 .build();
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        dispatcher.add(null, headers, child1);
    }

    @Test
    public void shouldAddDispatcherWithNullHeadersAndNullKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        dispatcher.add(null, null, child1);
    }

    @Test
    public void shouldAddMultipleDispatchersWithSameKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), null, child1);
        dispatcher.add(asOctets("key1"), null, child2);
    }

    @Test
    public void shouldDispatchWithoutKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(null, null, child1);
        dispatcher.add(null, null, child2);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, DirectBuffer> header = context.mock(Function.class, "header");

        context.checking(new Expectations()
        {
            {
                oneOf(child1).dispatch(with(1), with(10L), with(12L), with((DirectBuffer) null),
                        with(header), with((DirectBuffer) null));
                will(returnValue(1));
                oneOf(child2).dispatch(with(1), with(10L), with(12L), with((DirectBuffer) null),
                        with(header), with((DirectBuffer) null));
                will(returnValue(1));
            }
        });
        assertEquals(2, dispatcher.dispatch(1, 10L, 12L, null, header, null));
    }


    @Test
    public void shouldDispatchWithMatchingKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher child3 = context.mock(MessageDispatcher.class, "child3");
        dispatcher.add(asOctets("key1"), null, child1);
        dispatcher.add(asOctets("key1"), null, child2);
        dispatcher.add(asOctets("key2"), null, child3);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, DirectBuffer> header = context.mock(Function.class, "header");

        context.checking(new Expectations()
        {
            {
                oneOf(child1).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key1")),
                        with(header), with((DirectBuffer) null));
                will(returnValue(1));
                oneOf(child2).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key1")),
                        with(header), with((DirectBuffer) null));
                will(returnValue(1));
                oneOf(child3).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key2")),
                        with(header), with((DirectBuffer) null));
                will(returnValue(1));
            }
        });
        assertEquals(2, dispatcher.dispatch(1, 10L, 12L, asBuffer("key1"), header, null));
        assertEquals(1, dispatcher.dispatch(1, 10L, 12L, asBuffer("key2"), header, null));
    }

    @Test
    public void shouldNotDispatchNonMatchingKey()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), null, child1);
        dispatcher.add(asOctets("key1"), null, child2);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, DirectBuffer> header = context.mock(Function.class, "header");

        context.checking(new Expectations()
        {
            {
            }
        });
        assertEquals(0, dispatcher.dispatch(1, 10L, 12L, asBuffer("key2"), header, null));
    }

    @Test
    public void shouldFlush()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), null, child1);
        dispatcher.add(asOctets("key1"), null, child2);
        context.checking(new Expectations()
        {
            {
                oneOf(child1).flush(1, 10L, 12L);
                oneOf(child2).flush(1, 10L, 12L);
            }
        });
        dispatcher.flush(1, 10L, 12L);
    }

    @Test
    public void shouldRemoveDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(asOctets("key1"), null, child1);
        dispatcher.add(asOctets("key1"), null, child2);
        assertTrue(dispatcher.remove(asOctets("key1"), null, child1));
        assertTrue(dispatcher.remove(asOctets("key1"), null, child2));
        assertTrue(dispatcher.isEmpty());
    }

    @Test
    public void shouldNotRemoveDispatcherWhenNotPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");

        assertFalse(dispatcher.remove(asOctets("key1"), null, child1));

        dispatcher.add(asOctets("key1"), null, child1);
        assertFalse(dispatcher.remove(asOctets("key1"), null, child2));
    }

    private DirectBuffer asBuffer(String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        return new UnsafeBuffer(bytes);
    }

    private OctetsFW asOctets(String value)
    {
        DirectBuffer buffer = asBuffer(value);
        return new OctetsFW().wrap(buffer, 0, buffer.capacity());
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

}
