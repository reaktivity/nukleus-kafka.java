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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.collections.LongLongConsumer;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public final class HeaderMessageDispatcherTest
{
    private HeaderMessageDispatcher dispatcher = new HeaderMessageDispatcher(asOctets("headerName"));

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldAddNewDispatcherWhenAbsents()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        assertNull(dispatcher.addIfAbsent(asOctets("headerValue_1"), child1));
        assertNull(dispatcher.addIfAbsent(asOctets("headerValue_2"), child2));
    }

    @Test
    public void shouldNotAddDispatcherWhenPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher child3 = context.mock(MessageDispatcher.class, "child3");
        dispatcher.addIfAbsent(asOctets("headerValue_1"), child1);
        dispatcher.addIfAbsent(asOctets("headerValue_2"), child2);
        assertSame(child1, dispatcher.addIfAbsent(asOctets("headerValue_1"), child3));
        assertSame(child2, dispatcher.addIfAbsent(asOctets("headerValue_2"), child3));
    }

    @Test
    public void shouldDispatch()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        assertNull(dispatcher.addIfAbsent(asOctets("headerValue_1"), child1));
        assertNull(dispatcher.addIfAbsent(asOctets("headerValue_2"), child2));
        @SuppressWarnings("unchecked")
        Function<DirectBuffer, DirectBuffer> header = context.mock(Function.class, "header");
        LongLongConsumer ack = context.mock(LongLongConsumer.class, "ack");
        context.checking(new Expectations()
        {
            {
                oneOf(header).apply(with(bufferMatching("headerName")));
                will(returnValue(asBuffer("headerValue_2")));
                oneOf(child2).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key")),
                        with(header), with(ack), with((DirectBuffer) null));
                will(returnValue(1));
            }
        });
        assertEquals(1, dispatcher.dispatch(1, 10L, 12L, asBuffer("key"), header, ack, null));
    }

    @Test
    public void shouldNotDispatch()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.addIfAbsent(asOctets("headerValue_1"), child1);
        dispatcher.addIfAbsent(asOctets("headerValue_2"), child2);
        @SuppressWarnings("unchecked")
        Function<DirectBuffer, DirectBuffer> header = context.mock(Function.class, "header");
        LongLongConsumer ack = context.mock(LongLongConsumer.class, "ack");
        context.checking(new Expectations()
        {
            {
                oneOf(header).apply(with(bufferMatching("headerName")));
                will(returnValue(asBuffer("headerValue_3")));
            }
        });
        assertEquals(0, dispatcher.dispatch(1, 10L, 12L, null, header, ack, null));
    }

    @Test
    public void shouldRemoveDispatcherWhenPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.addIfAbsent(asOctets("headerValue_1"), child1);
        dispatcher.addIfAbsent(asOctets("headerValue_2"), child2);
        assertSame(child1, dispatcher.remove(asOctets("headerValue_1")));
        assertSame(child2, dispatcher.remove(asOctets("headerValue_2")));
    }

    @Test
    public void shouldNotRemoveDispatcherWhenNotPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.addIfAbsent(asOctets("headerValue_1"), child1);
        dispatcher.addIfAbsent(asOctets("headerValue_2"), child2);
        assertNull(dispatcher.remove(asOctets("headerValue_3")));
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
