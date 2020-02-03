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
package org.reaktivity.nukleus.kafka.internal.budget;

import static org.reaktivity.nukleus.budget.BudgetDebitor.NO_DEBITOR_INDEX;

import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongArrayList;
import org.reaktivity.nukleus.budget.BudgetDebitor;

final class KafkaMergedBudget
{
    private final long budgetId;
    private final long mergedWatcherId;
    private final LongConsumer mergedFlusher;
    private final Long2ObjectHashMap<LongConsumer> flushers;
    private final LongArrayList watchers;

    private long debitorIndex = NO_DEBITOR_INDEX;
    private BudgetDebitor debitor;
    private long budget;
    private int watcherIndex;

    KafkaMergedBudget(
        long budgetId,
        long mergedWatcherId)
    {
        this.budgetId = budgetId;
        this.mergedWatcherId = mergedWatcherId;
        this.mergedFlusher = this::flush;
        this.flushers = new Long2ObjectHashMap<>();
        this.watchers = new LongArrayList();
    }

    long credit(
        long traceId,
        long credit)
    {
        final long budgetSnapshot = budget;

        budget += credit;

        flush(traceId);

        return budgetSnapshot;
    }

    int claim(
        long watcherId,
        int minimum,
        int maximum)
    {
        final int budgetMax = (int)(Math.min(budget, Integer.MAX_VALUE) & 0x7fff_ffff);

        int claimed = Math.min(budgetMax, maximum);
        if (claimed >= minimum && debitorIndex != NO_DEBITOR_INDEX)
        {
            claimed = debitor.claim(debitorIndex, mergedWatcherId, minimum, maximum);
        }

        final int watcherAt = watchers.indexOf(watcherId);
        if (claimed >= minimum)
        {
            if (watcherAt != -1)
            {
                watchers.remove(watcherAt);
            }
            budget -= claimed;
        }
        else
        {
            if (watcherAt == -1)
            {
                watchers.addLong(watcherId);
            }
            claimed = 0;
        }

        return claimed;
    }

    void attach(
        long watcherId,
        LongConsumer flusher,
        LongFunction<BudgetDebitor> supplyDebitor)
    {
        if (budgetId != 0L && debitorIndex == NO_DEBITOR_INDEX)
        {
            debitor = supplyDebitor.apply(budgetId);
            debitorIndex = debitor.acquire(budgetId, mergedWatcherId, mergedFlusher);
        }

        flushers.put(watcherId, flusher);
    }

    void detach(
        long watcherId)
    {
        watchers.removeLong(watcherId);
        flushers.remove(watcherId);

        if (flushers.isEmpty() &&
            budgetId != 0L && debitorIndex != NO_DEBITOR_INDEX)
        {
            assert watchers.isEmpty();

            debitor.release(debitorIndex, mergedWatcherId);
            debitor = null;
            debitorIndex = NO_DEBITOR_INDEX;
        }
    }

    private void flush(
        long traceId)
    {
        if (!watchers.isEmpty())
        {
            if (watcherIndex >= watchers.size())
            {
                watcherIndex = 0;
            }

            for (int index = watcherIndex; index < watchers.size(); index++)
            {
                final long watcherId = watchers.getLong(index);
                final LongConsumer flusher = flushers.get(watcherId);
                flusher.accept(traceId);
            }

            if (watcherIndex >= watchers.size())
            {
                watcherIndex = 0;
            }

            for (int index = 0; index < watcherIndex; index++)
            {
                final long watcherId = watchers.getLong(index);
                final LongConsumer flusher = flushers.get(watcherId);
                flusher.accept(traceId);
            }

            watcherIndex++;
        }
    }
}
