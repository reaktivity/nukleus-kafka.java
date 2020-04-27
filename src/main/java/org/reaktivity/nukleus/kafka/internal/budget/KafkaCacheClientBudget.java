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

import org.agrona.collections.LongHashSet;
import org.reaktivity.nukleus.budget.BudgetCreditor;

public final class KafkaCacheClientBudget
{
    public final long topicBudgetId;

    private final BudgetCreditor creditor;
    private final LongHashSet partitionIds;

    private long topicBudgetIndex = NO_CREDITOR_INDEX;

    public KafkaCacheClientBudget(
        BudgetCreditor creditor,
        long creditorId)
    {
        this.creditor = creditor;
        this.topicBudgetId = creditorId;
        this.partitionIds = new LongHashSet();
    }

    public long acquire(
        int partitionId)
    {
        if (partitionIds.isEmpty())
        {
            assert topicBudgetIndex == NO_CREDITOR_INDEX;
            this.topicBudgetIndex = creditor.acquire(topicBudgetId);
        }

        partitionIds.add(partitionId);
        assert topicBudgetIndex != NO_CREDITOR_INDEX;

        return partitionId;
    }

    public void release(
        long partitionId,
        int partitionBudget)
    {
        // TODO: topicBudgetMax = buffer slot capacity
        creditor.credit(0L, topicBudgetIndex, -partitionBudget);

        partitionIds.remove(partitionId);

        if (partitionIds.isEmpty())
        {
            creditor.release(topicBudgetIndex);
            this.topicBudgetIndex = NO_CREDITOR_INDEX;
        }
    }

    public long credit(
        long traceId,
        long partitionId,
        long partitionCredit)
    {
        // TODO: topicBudgetMax = buffer slot capacity
        return creditor.credit(0L, topicBudgetIndex, partitionCredit);
    }
}
