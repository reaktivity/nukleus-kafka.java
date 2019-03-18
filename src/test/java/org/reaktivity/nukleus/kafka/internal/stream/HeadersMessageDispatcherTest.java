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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.api.Action;
import org.jmock.api.Invocation;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class HeadersMessageDispatcherTest
{
    private final KafkaHeaderFW.Builder headerRW = new KafkaHeaderFW.Builder();
    private final KafkaHeaderFW.Builder header1RW = new KafkaHeaderFW.Builder();
    private final KafkaHeaderFW.Builder header2RW = new KafkaHeaderFW.Builder();

    private final MutableDirectBuffer headersBuffer = new UnsafeBuffer(new byte[1000]);
    private final MutableDirectBuffer headers1Buffer = new UnsafeBuffer(new byte[1000]);
    private final MutableDirectBuffer headers2Buffer = new UnsafeBuffer(new byte[1000]);

    private Iterator<KafkaHeaderFW> emptyHeaders = Collections.emptyIterator();

    private HeadersMessageDispatcher dispatcher = new HeadersMessageDispatcher("", HeaderValueMessageDispatcher::new);

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();


    @Test
    public void shouldAddNewDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(emptyHeaders, child1);
        Iterator<KafkaHeaderFW> headers = Collections.singletonList(
                headerRW.wrap(headersBuffer, 0, headersBuffer.capacity())
                        .key("header2").value(asOctets("value2"))
                        .build()).iterator();
        dispatcher.add(headers, child2);
    }


    @Test
    public void shouldAdjustOffset()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(emptyHeaders, child1);
        Iterator<KafkaHeaderFW> headers = Collections.singletonList(
                headerRW.wrap(headersBuffer, 0, headersBuffer.capacity())
                        .key("header2").value(asOctets("value2"))
                        .build()).iterator();
        dispatcher.add(headers, child2);

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
        dispatcher.add(emptyHeaders, child1);
        Iterator<KafkaHeaderFW> headers = Collections.singletonList(
                headerRW.wrap(headersBuffer, 0, headersBuffer.capacity())
                        .key("header2").value(asOctets("value2"))
                        .build()).iterator();
        dispatcher.add(headers, child2);

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
    public void shouldNotDispatchWhenNoMatchingHeaders()
    {

        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");

        List<KafkaHeaderFW> headers1 = Collections.singletonList(
                header1RW.wrap(headers1Buffer, 0, headers1Buffer.capacity())
                         .key("header1").value(asOctets("value1"))
                         .build());
        List<KafkaHeaderFW> headers2 = Collections.singletonList(
                header2RW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
                         .key("header2").value(asOctets("value2"))
                         .build());
        dispatcher.add(headers1.iterator(), child1);
        dispatcher.add(headers2.iterator(), child2);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, Iterator<DirectBuffer>> supplyHeader = context.mock(Function.class, "header");

        context.checking(new Expectations()
        {
            {
                oneOf(supplyHeader).apply(with(bufferMatching("header1")));
                will(returnValue(Collections.singletonList(asBuffer("no match")).iterator()));
                oneOf(supplyHeader).apply(with(bufferMatching("header2")));
                will(returnValue(Collections.singletonList(asBuffer("no match")).iterator()));
            }
        });

        int result = dispatcher.dispatch(1, 10L, 12L, asBuffer("key"), supplyHeader, 123L, 0L, null);

        assertEquals(0, result);
    }

    @Test
    public void shouldFlush()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");

        List<KafkaHeaderFW> headers1 = Collections.singletonList(
                header1RW.wrap(headers1Buffer, 0, headers1Buffer.capacity())
                        .key("header1").value(asOctets("value1"))
                        .build());
        List<KafkaHeaderFW> headers2 = Collections.singletonList(
                header2RW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
                        .key("header2").value(asOctets("value2"))
                        .build());
        dispatcher.add(headers1.iterator(), child1);
        dispatcher.add(headers2.iterator(), child2);


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
    public void shouldRemoveDispatcherWhenPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        List<KafkaHeaderFW> headers = Collections.singletonList(
                headerRW.wrap(headersBuffer, 0, headersBuffer.capacity())
                        .key("header1").value(asOctets("value1"))
                        .build());
        dispatcher.add(headers.iterator(), child1);
        boolean removed = dispatcher.remove(headers.iterator(), child1);

        assertTrue(removed);
        assertTrue(dispatcher.isEmpty());
    }


    @Test
    public void shouldNotRemoveDispatcherWhenNotPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        List<KafkaHeaderFW> headers = Collections.singletonList(
                header1RW.wrap(headers1Buffer, 0, headersBuffer.capacity())
                        .key("header1").value(asOctets("value1"))
                        .build());
        dispatcher.add(headers.iterator(), child1);

        List<KafkaHeaderFW> headers2 = Collections.singletonList(
                header2RW.wrap(headers2Buffer, 0, headersBuffer.capacity())
                        .key("header2").value(asOctets("value1"))
                        .build());
        boolean removed = dispatcher.remove(headers2.iterator(), child1);

        assertFalse(removed);
        assertFalse(dispatcher.isEmpty());
    }


    @Test
    public void shouldAccountDeferredUpdates()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers1 = Collections.singletonList(
                header1RW.wrap(headers1Buffer, 0, headers1Buffer.capacity())
                         .key("header1").value(asOctets("value1"))
                         .build());
        dispatcher.add(headers1.iterator(), child1);

        context.checking(new Expectations()
        {
            {
                oneOf(child1).flush(1, 10L, 12L);
                will(removeChild(dispatcher, headers1.iterator(), child1));
                oneOf(child2).flush(2, 20L, 13L);
            }
        });
        dispatcher.flush(1, 10L, 12L);

        // No dispatchers (child1 should be removed by now)
        dispatcher.add(headers1.iterator(), child2);
        dispatcher.flush(2, 20L, 13L);
    }

    private static Action removeChild(
        HeadersMessageDispatcher dispatcher,
        Iterator<KafkaHeaderFW> headers,
        MessageDispatcher child)
    {
        return new Action()
        {
            @Override
            public void describeTo(Description description)
            {
            }

            @Override
            public Object invoke(Invocation invocation)
            {
                dispatcher.remove(headers, child);
                return null;
            }
        };
    }

    private static DirectBuffer asBuffer(String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        return new UnsafeBuffer(bytes);
    }

    private static OctetsFW asOctets(String value)
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
                return item instanceof UnsafeBuffer && item.equals(new UnsafeBuffer(string.getBytes(UTF_8)));
            }

            @Override
            public void describeTo(Description description)
            {
                description.appendText(string);
            }

        };
    }

}
