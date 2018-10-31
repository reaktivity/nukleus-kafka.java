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
import static org.reaktivity.nukleus.kafka.internal.stream.MessageDispatcher.FLAGS_MATCHED;

import java.util.Iterator;
import java.util.function.Function;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

public final class BroadcastMessageDispatcherTest
{
    private BroadcastMessageDispatcher dispatcher = new BroadcastMessageDispatcher();

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldAddDispatchers()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(child1);
        dispatcher.add(child2);
    }

    @Test
    public void shouldAdjustOffset()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(child1);
        dispatcher.add(child2);
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
        dispatcher.add(child1);
        dispatcher.add(child2);
        context.checking(new Expectations()
        {
            {
                oneOf(child1).detach(false);
                oneOf(child2).detach(false);
            }
        });
        dispatcher.detach(false);
    }

    @Test
    public void shouldDispatch()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(child1);
        dispatcher.add(child2);
        final long timestamp = System.currentTimeMillis() - 123;
        final long traceId = 0L;
        @SuppressWarnings("unchecked")
        Function<DirectBuffer, Iterator<DirectBuffer>> header = context.mock(Function.class, "header");
        context.checking(new Expectations()
        {
            {
                oneOf(child1).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key")),
                        with(header), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_DELIVERED));
                oneOf(child2).dispatch(with(1), with(10L), with(12L), with(bufferMatching("key")),
                        with(header), with(timestamp), with(traceId), with((DirectBuffer) null));
                will(returnValue(FLAGS_MATCHED));
            }
        });
        assertEquals(FLAGS_DELIVERED | FLAGS_MATCHED,
                dispatcher.dispatch(1, 10L, 12L, asBuffer("key"), header, timestamp, traceId, null));
    }

    @Test
    public void shouldFlush()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        dispatcher.add(child1);
        dispatcher.add(child2);
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
        dispatcher.add(child1);
        dispatcher.add(child2);
        assertTrue(dispatcher.remove(child1));
        assertTrue(dispatcher.remove(child2));
    }

    @Test
    public void shouldNotRemoveDispatcherWhenNotPresent()
    {
        MessageDispatcher child1 = context.mock(MessageDispatcher.class, "child1");
        MessageDispatcher child2 = context.mock(MessageDispatcher.class, "child2");
        MessageDispatcher another = context.mock(MessageDispatcher.class, "another");
        dispatcher.add(child1);
        dispatcher.add(child2);
        assertFalse(dispatcher.remove(another));
    }

    private DirectBuffer asBuffer(String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        return new UnsafeBuffer(bytes);
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
