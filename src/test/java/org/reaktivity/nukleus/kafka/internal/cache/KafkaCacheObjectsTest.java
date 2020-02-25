/**
 * Copyright 2016-2020 The Reaktivity Project
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheObjects.ReadOnly;
import org.reaktivity.nukleus.kafka.internal.cache.KafkaCacheObjects.ReadWrite;

public class KafkaCacheObjectsTest
{
    @Rule
    public final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    @Test
    public void shouldCloseWriterOnly() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(1);

        TestReadWrite writer = new TestReadWrite(latch);

        writer.close();

        latch.await();

        assertTrue(writer.closed());
    }

    @Test
    public void shouldCloseWriterWithReader() throws Exception
    {
        CountDownLatch latch = new CountDownLatch(2);

        TestReadWrite writer = new TestReadWrite(latch);
        TestReadOnly reader = writer.acquire(TestReadOnly::new);

        reader.close();
        writer.close();

        latch.await();

        assertTrue(reader.closed());
        assertTrue(writer.closed());
    }

    @Test
    public void shouldCloseWriterWithParallelReaders() throws Exception
    {
        int readerCount = 10;

        CountDownLatch latch = new CountDownLatch(readerCount + 1);

        ExecutorService executor = Executors.newFixedThreadPool(readerCount + 1);
        try
        {
            TestReadWrite writer = new TestReadWrite(latch);
            List<TestReadOnly> readers = new ArrayList<>(readerCount);
            for (int i = readerCount; i >= 0; i--)
            {
                TestReadOnly reader = writer.acquire(TestReadOnly::new);
                readers.add(reader);
            }

            readers.forEach(reader -> executor.submit(reader::close));
            writer.close();

            latch.await();

            readers.forEach(reader -> assertTrue(reader.closed()));
            assertTrue(writer.closed());
        }
        finally
        {
            executor.shutdownNow();
        }
    }

    private static final class TestReadWrite extends ReadWrite<TestReadOnly, TestReadWrite>
    {
        private final CountDownLatch latch;

        private TestReadWrite(
            CountDownLatch latch)
        {
            this.latch = latch;
        }

        @Override
        protected TestReadWrite self()
        {
            return this;
        }

        @Override
        protected void onClosed()
        {
            assert latch.getCount() > 0;
            latch.countDown();
        }
    }

    private static final class TestReadOnly extends ReadOnly
    {
        private final CountDownLatch latch;

        private TestReadOnly(
            TestReadWrite writer)
        {
            super(writer);
            this.latch = writer.latch;
        }

        @Override
        protected void onClosed()
        {
            assert latch.getCount() > 0;
            latch.countDown();
        }
    }
}
