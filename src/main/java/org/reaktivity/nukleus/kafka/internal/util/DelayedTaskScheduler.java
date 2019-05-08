/**
 * Copyright 2016-2019 The Reaktivity Project
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
package org.reaktivity.nukleus.kafka.internal.util;

import org.agrona.DeadlineTimerWheel;
import org.agrona.DeadlineTimerWheel.TimerHandler;
import org.agrona.collections.Long2ObjectHashMap;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.agrona.DeadlineTimerWheel.NULL_TIMER;

import java.util.concurrent.TimeUnit;

public class DelayedTaskScheduler
{
    private final DeadlineTimerWheel timerWheel;
    private final Long2ObjectHashMap<Runnable> tasksByTimerId;
    private final TimerHandler handleTimeout;

    public DelayedTaskScheduler()
    {
        this.timerWheel = new DeadlineTimerWheel(MILLISECONDS, currentTimeMillis(), 512, 32);
        this.tasksByTimerId = new Long2ObjectHashMap<>();
        this.handleTimeout = this::onTimeout;
    }

    public long newTimeout(
        long delay,
        Runnable task)
    {
        final long nowMillis = System.currentTimeMillis();
        final long timerId = timerWheel.scheduleTimer(nowMillis + delay);

        tasksByTimerId.put(timerId, task);

        return timerId;
    }

    public long rescheduleTimeout(
        long delay,
        long timerId)
    {
        if (timerId != NULL_TIMER)
        {
            final Runnable task = tasksByTimerId.remove(timerId);
            assert task != null;

            timerWheel.cancelTimer(timerId);

            timerId = newTimeout(delay, task);
        }

        return timerId;
    }

    public long rescheduleTimeout(
        long delay,
        long timerId,
        Runnable task)
    {
        cancel(timerId);
        return newTimeout(delay, task);
    }

    public long cancel(
        long timerId)
    {
        if (timerWheel.cancelTimer(timerId))
        {
            tasksByTimerId.remove(timerId);
        }

        return NULL_TIMER;
    }

    public int process()
    {
        final long nowMillis = System.currentTimeMillis();

        return timerWheel.poll(nowMillis, handleTimeout, Integer.MAX_VALUE);
    }

    private boolean onTimeout(
        TimeUnit unit,
        long now,
        long timerId)
    {
        final Runnable task = tasksByTimerId.remove(timerId);
        if (task != null)
        {
            task.run();
        }
        return task != null;
    }
}
