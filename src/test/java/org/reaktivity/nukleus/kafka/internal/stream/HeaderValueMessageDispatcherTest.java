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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.reaktivity.nukleus.kafka.internal.stream.MessageDispatcher.FLAGS_DELIVERED;

import java.util.ArrayList;
import java.util.List;
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
import org.reaktivity.nukleus.kafka.internal.types.KafkaHeaderFW;
import org.reaktivity.nukleus.kafka.internal.types.ListFW;
import org.reaktivity.nukleus.kafka.internal.types.OctetsFW;

public final class HeaderValueMessageDispatcherTest
{
    private final ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW> headersRW =
         new ListFW.Builder<KafkaHeaderFW.Builder, KafkaHeaderFW>(new KafkaHeaderFW.Builder(), new KafkaHeaderFW());
    private final MutableDirectBuffer headers2Buffer = new UnsafeBuffer(new byte[1000]);
    private final MutableDirectBuffer headers3Buffer = new UnsafeBuffer(new byte[1000]);

    private HeaderValueMessageDispatcher dispatcher = new HeaderValueMessageDispatcher(asBuffer("header1"));

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldAddNewDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers1 = new ArrayList<>();
        dispatcher.add(asOctets("header1"), headers1.iterator(), child1);
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        dispatcher.add(asOctets("header1"), headers2.iterator(), child2);
    }

    @Test
    public void shouldGetDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers1 = new ArrayList<>();
        dispatcher.add(asOctets("value1"), headers1.iterator(), child1);
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        dispatcher.add(asOctets("value1"), headers2.iterator(), child2);
        assertNotNull(dispatcher.get(asOctets("value1")));
        assertNull(dispatcher.get(asOctets("value2")));
    }

    @Test
    public void shouldDispatchOneAndTwoMatchingHeaders()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers1 = new ArrayList<>();
        dispatcher.add(asOctets("value1"), headers1.iterator(), child1);
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        dispatcher.add(asOctets("value1"), headers2.iterator(), child2);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, DirectBuffer> supplyHeader = context.mock(Function.class, "header");

        final long timestamp = System.currentTimeMillis() - 123;
        final long traceId = 0L;

        context.checking(new Expectations()
        {
            {
                oneOf(supplyHeader).apply(with(bufferMatching("header1")));
                will(returnValue(asBuffer("value1")));
                oneOf(supplyHeader).apply(with(bufferMatching("header2")));
                will(returnValue(asBuffer("value2")));
                oneOf(child1).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key")),
                        with(supplyHeader), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
                oneOf(child2).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key")),
                        with(supplyHeader), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
            }
        });
        assertEquals(FLAGS_DELIVERED, dispatcher.dispatch(1, 10L, 12L, asBuffer("key"), supplyHeader, timestamp, traceId, null));
    }

    @Test
    public void shouldDispatchOnlyOneWithHeader2Absent()
    {

        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers1 = new ArrayList<>();
        dispatcher.add(asOctets("value1"), headers1.iterator(), child1);
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        dispatcher.add(asOctets("value1"), headers2.iterator(), child2);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, DirectBuffer> supplyHeader = context.mock(Function.class, "header");

        final long timestamp = System.currentTimeMillis() - 123;
        final long traceId = 0L;

        context.checking(new Expectations()
        {
            {
                oneOf(supplyHeader).apply(with(bufferMatching("header1")));
                will(returnValue(asBuffer("value1")));
                oneOf(supplyHeader).apply(with(bufferMatching("header2")));
                will(returnValue(null));
                oneOf(child1).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key")),
                        with(supplyHeader), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
            }
        });
        assertEquals(FLAGS_DELIVERED, dispatcher.dispatch(1, 10L, 12L, asBuffer("key"), supplyHeader, timestamp, traceId, null));
    }

    @Test
    public void shouldNotDispatchWhenNoMatchingHeaders()
    {

        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers1 = new ArrayList<>();
        dispatcher.add(asOctets("value1"), headers1.iterator(), child1);
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        dispatcher.add(asOctets("value1"), headers2.iterator(), child2);

        @SuppressWarnings("unchecked")
        Function<DirectBuffer, DirectBuffer> supplyHeader = context.mock(Function.class, "header");

        context.checking(new Expectations()
        {
            {
                oneOf(supplyHeader).apply(with(bufferMatching("header1")));
                will(returnValue(asBuffer("no match")));
            }
        });
        assertEquals(0, dispatcher.dispatch(1, 10L, 12L, asBuffer("key"), supplyHeader, 123L, 0L, null));
    }

    @Test
    public void shouldFlush()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers1 = new ArrayList<>();
        dispatcher.add(asOctets("value1"), headers1.iterator(), child1);
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        dispatcher.add(asOctets("value1"), headers2.iterator(), child2);

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
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        List<KafkaHeaderFW> headers1 = new ArrayList<>();
        dispatcher.add(asOctets("value1"), headers1.iterator(), child1);
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        dispatcher.add(asOctets("value1"), headers2.iterator(), child2);

        assertTrue(dispatcher.remove(asOctets("value1"), headers2.iterator(), child2));
        assertFalse(dispatcher.isEmpty());

        assertTrue(dispatcher.remove(asOctets("value1"), headers1.iterator(), child1));
        assertTrue(dispatcher.isEmpty());
    }

    @Test
    public void shouldNotRemoveDispatcherWhenNotPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        assertFalse(dispatcher.remove(asOctets("header1"), headers2.iterator(), child1));
    }

    @Test
    public void shouldToString()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher child3 = context.mock(MessageDispatcher.class, "child3");
        List<KafkaHeaderFW> headers1 = new ArrayList<>();
        dispatcher.add(asOctets("value1"), headers1.iterator(), child1);
        List<KafkaHeaderFW> headers2 = new ArrayList<>();
        headersRW.wrap(headers2Buffer, 0, headers2Buffer.capacity())
            .item(b -> b.key("header2").value(asOctets("value2")))
            .build()
            .forEach(h -> headers2.add(h));
        dispatcher.add(asOctets("value1"), headers2.iterator(), child2);
        List<KafkaHeaderFW> headers3 = new ArrayList<>();
                headersRW.wrap(headers3Buffer, 0, headers3Buffer.capacity())
                .item(b -> b.key("header2").value(asOctets("value2")))
                .build()
                .forEach(h -> headers3.add(h));
        dispatcher.add(asOctets("value2"), headers3.iterator(), child3);
        String result = dispatcher.toString();
        assertTrue(result.contains(child1.toString()));
        assertTrue(result.contains(child2.toString()));
        assertTrue(result.contains(child3.toString()));
        assertTrue(result.contains("header1"));
        assertTrue(result.contains("header2"));
        assertTrue(result.contains("value1"));
        assertTrue(result.contains("value2"));
        System.out.println(result);
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
