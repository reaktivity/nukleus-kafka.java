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

import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import org.agrona.collections.Long2ObjectHashMap;
import org.reaktivity.nukleus.budget.BudgetDebitor;

final class KafkaMergedBudgetDebitor implements BudgetDebitor
{
    private final Long2ObjectHashMap<KafkaMergedBudget> budgetsByMergedId;
    private final LongFunction<BudgetDebitor> supplyDebitor;

    KafkaMergedBudgetDebitor(
        Long2ObjectHashMap<KafkaMergedBudget> budgetsByMergedId,
        LongFunction<BudgetDebitor> supplyDebitor)
    {
        this.budgetsByMergedId = budgetsByMergedId;
        this.supplyDebitor = supplyDebitor;
    }

    @Override
    public long acquire(
        long mergedBudgetId,
        long watcherId,
        LongConsumer flusher)
    {
        long budgetIndex = NO_DEBITOR_INDEX;

        final KafkaMergedBudget mergedBudget = budgetsByMergedId.get(mergedBudgetId);
        if (mergedBudget != null)
        {
            mergedBudget.attach(watcherId, flusher, supplyDebitor);
            budgetIndex = mergedBudgetId;
        }

        return budgetIndex;
    }

    @Override
    public int claim(
        long mergedBudgetId,
        long watcherId,
        int minimum,
        int maximum)
    {
        final KafkaMergedBudget mergedBudget = budgetsByMergedId.get(mergedBudgetId);
        assert mergedBudget != null;

        return mergedBudget.claim(watcherId, minimum, maximum);
    }

    @Override
    public void release(
        long mergedBudgetId,
        long watcherId)
    {
        final KafkaMergedBudget mergedBudget = budgetsByMergedId.get(mergedBudgetId);
        assert mergedBudget != null;

        mergedBudget.detach(watcherId);
    }
}