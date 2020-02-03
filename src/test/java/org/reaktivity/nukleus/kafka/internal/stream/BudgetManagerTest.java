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
package org.reaktivity.nukleus.kafka.internal.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;


import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;

public final class BudgetManagerTest
{
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    public Runnable stream1 = context.mock(Runnable.class, "stream1");
    public Runnable stream2 = context.mock(Runnable.class, "stream2");

    private final BudgetManager budgetManager = new BudgetManager();

    @Test
    public void shouldCreateIndependantBudgetsForGroup0()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(stream1).run();
                oneOf(stream2).run();
            }
        });

        Budget budget1 = budgetManager.createBudget(0L);
        budget1.incBudget(1L, 100, stream1);

        Budget budget2 = budgetManager.createBudget(0L);
        budget2.incBudget(2L, 200, stream2);

        assertEquals(100, budget1.getBudget());
        assertEquals(200, budget2.getBudget());

        budget1.decBudget(1L, 60);
        assertEquals(40, budget1.getBudget());

        budget2.decBudget(1L, 50);
        assertEquals(150, budget2.getBudget());
    }

    @Test
    public void shouldCreditInitialBudgetOnlyforVeryFirstStream()
    {
        context.checking(new Expectations()
        {
            {
                oneOf(stream1).run();
            }
        });

        Budget budget1 = budgetManager.createBudget(1L);
        budget1.incBudget(1L, 100, stream1);
        context.assertIsSatisfied();

        context.checking(new Expectations()
        {
            {
                oneOf(stream1).run();
                oneOf(stream2).run();
            }
        });

        Budget budget2 = budgetManager.createBudget(1L);
        budget2.incBudget(2L, 200, stream2);

        assertEquals(100, budget1.getBudget());
        assertEquals(100, budget2.getBudget());
    }

    @Test
    public void shouldCreateDependantBudgetsForNonZeroGroup()
    {
        context.checking(new Expectations()
        {
            {
                exactly(2).of(stream1).run();
                oneOf(stream2).run();
            }
        });

        Budget budget1 = budgetManager.createBudget(1L);
        budget1.incBudget(1L, 100, stream1);

        Budget budget2 = budgetManager.createBudget(1L);
        budget2.incBudget(2L, 100, stream2);

        assertSame(budget1, budget2);

        assertEquals(100, budget1.getBudget());
        assertEquals(100, budget2.getBudget());

        context.checking(new Expectations()
        {
            {
                oneOf(stream2).run();
            }
        });

        budget1.decBudget(1L, 60);
        assertEquals(40, budget1.getBudget());

        context.checking(new Expectations()
        {
            {
                oneOf(stream1).run();
            }
        });
        budget1.closing(1L, 60);
        assertEquals(100, budget1.getBudget());

        budget1.closed(1L);
    }

}
