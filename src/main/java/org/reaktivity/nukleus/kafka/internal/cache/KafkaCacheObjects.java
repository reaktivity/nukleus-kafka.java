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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public final class KafkaCacheObjects
{
    public abstract static class ReadWrite<R extends ReadOnly<R, W>, W extends ReadWrite<R, W>> implements AutoCloseable
    {
        private final AtomicInteger references;
        private volatile boolean closed;
        private volatile boolean closing;

        protected ReadWrite()
        {
            this.references = new AtomicInteger(1);
        }

        protected abstract W self();

        public final R acquire(
            Function<W, R> factory)
        {
            assert !closed;
            references.incrementAndGet();
            return factory.apply(self());
        }

        @Override
        public final void close()
        {
            if (!closing)
            {
                closing = true;
                release();
            }
        }

        public final boolean closed()
        {
            return closed;
        }

        protected final int references()
        {
            return references.get();
        }

        protected abstract void onClosed();

        final void release()
        {
            final int count = references.decrementAndGet();
            assert count >= 0;
            if (count == 0)
            {
                assert !closed;
                closed = true;
                onClosed();
            }
        }
    }

    public abstract static class ReadOnly<R extends ReadOnly<R, W>, W extends ReadWrite<R, W>> implements AutoCloseable
    {
        private final W referee;
        private volatile boolean closed;
        private volatile int references;

        protected ReadOnly(
            W referee)
        {
            this.referee = referee;
            this.references = 1;
        }

        protected abstract R self();

        public final R acquire()
        {
            assert !closed;
            references++;
            return self();
        }

        @Override
        public final void close()
        {
            if (!closed && --references == 0)
            {
                closed = true;
                onClosed();
                referee.release();
            }
        }

        public final boolean closed()
        {
            return closed;
        }

        protected final int references()
        {
            return references;
        }

        protected abstract void onClosed();
    }

    private KafkaCacheObjects()
    {
        // no instances
    }
}
