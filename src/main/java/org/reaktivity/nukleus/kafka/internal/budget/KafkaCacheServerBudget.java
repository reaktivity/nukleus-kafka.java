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

import static org.reaktivity.nukleus.budget.BudgetCreditor.NO_CREDITOR_INDEX;

import org.agrona.collections.Long2LongHashMap;
import org.reaktivity.nukleus.budget.BudgetCreditor;

public final class KafkaCacheServerBudget
{
    private static final long NO_BUDGET_ID = -1L;

    public final long sharedBudgetId;

    private final BudgetCreditor creditor;
    private final Long2LongHashMap unsharedBudgetsById;

    private long sharedBudgetIndex = NO_CREDITOR_INDEX;
    private long sharedBudget;

    public KafkaCacheServerBudget(
        BudgetCreditor creditor,
        long creditorId)
    {
        this.creditor = creditor;
        this.sharedBudgetId = creditorId;
        this.unsharedBudgetsById = new Long2LongHashMap(NO_BUDGET_ID);
    }

    public long acquire(
        long unsharedBudgetId)
    {
        if (unsharedBudgetsById.isEmpty())
        {
            assert sharedBudgetIndex == NO_CREDITOR_INDEX;
            this.sharedBudgetIndex = creditor.acquire(sharedBudgetId);
        }

        unsharedBudgetsById.put(unsharedBudgetId, 0L);
        assert sharedBudgetIndex != NO_CREDITOR_INDEX;

        return unsharedBudgetId;
    }

    public void release(
        long unsharedBudgetId)
    {
        unsharedBudgetsById.remove(unsharedBudgetId);

        if (unsharedBudgetsById.isEmpty())
        {
            creditor.release(sharedBudgetIndex);
            this.sharedBudgetIndex = NO_CREDITOR_INDEX;
        }
        else
        {
            flushSharedCreditIfNecessary(0L);
        }
    }

    public void credit(
        long traceId,
        long budgetId,
        long credit)
    {
        long budget = unsharedBudgetsById.get(budgetId);
        assert budget != NO_BUDGET_ID;
        budget += credit;
        assert budget >= 0L;
        unsharedBudgetsById.put(budgetId, budget);

        flushSharedCreditIfNecessary(traceId);
    }

    private void flushSharedCreditIfNecessary(
        long traceId)
    {
        final long sharedCredit = unsharedBudgetsById.minValue() - sharedBudget;
        sharedBudget += sharedCredit;

        if (sharedCredit > 0)
        {
            creditor.credit(traceId, sharedBudgetIndex, sharedCredit);
        }
    }
}
