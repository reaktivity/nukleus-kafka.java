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

import org.agrona.collections.Long2ObjectHashMap;

public class BudgetManager
{
    public static final Budget NO_BUDGET = new StreamBudget();

    private final Long2ObjectHashMap<GroupBudget> groups;             // group id -> GroupBudget

    private static class GroupStreamBudget
    {
        final long streamId;
        int unackedBudget;
        Runnable budgetAvailable;
        boolean closing;

        GroupStreamBudget(long streamId, Runnable budgetAvailable)
        {
            this.streamId = streamId;
            this.budgetAvailable = budgetAvailable;
        }

        @Override
        public String toString()
        {
            return String.format("(id=%d kind=%s closing=%s unackedBudget=%d)", streamId, closing, unackedBudget);
        }
    }

    private class GroupBudget implements Budget
    {
        final long groupId;
        final Long2ObjectHashMap<GroupStreamBudget> streamMap;          // stream id -> BudgetEnty

        private int initialBudget =  -1;
        private int budget;
        private int startPosition = -1;

        GroupBudget(long groupId)
        {
            this.groupId = groupId;
            this.streamMap = new Long2ObjectHashMap<>();
        }

        GroupStreamBudget get(long streamId)
        {
            return streamMap.get(streamId);
        }

        private void moreBudget(int credit)
        {
            budget += credit;
            if (budget > initialBudget)
            {
                initialBudget = budget;
            }
            assert budget <= initialBudget;

            // For fairness iterate from a moving startPosition to end, then 0 to startPosition
            startPosition = ++startPosition >= streamMap.size() ? 0 : startPosition;

            int pos = 0;
            for (GroupStreamBudget stream : streamMap.values())
            {
                if (pos >= startPosition)
                {
                    stream.budgetAvailable.run();
                }
                if (budget <= 0)
                {
                    break;
                }
                pos++;
            }

            pos = 0;
            for (GroupStreamBudget stream : streamMap.values())
            {
                if (pos >= startPosition || budget <= 0)
                {
                    break;
                }
                stream.budgetAvailable.run();
                pos++;
            }
        }

        @Override
        public void closed(long streamId)
        {
            GroupStreamBudget streamBudget = streamMap.remove(streamId);
            if (streamMap.isEmpty())
            {
                groups.remove(groupId);
            }
            else if (streamBudget != null && streamBudget.unackedBudget > 0)
            {
                moreBudget(streamBudget.unackedBudget);
            }
        }

        @Override
        public void closing(long streamId, int credit)
        {
            GroupStreamBudget streamBudget = streamMap.get(streamId);
            if (streamBudget != null)
            {
                streamBudget.unackedBudget -= credit;
                streamBudget.closing = true;
                if (credit > 0)
                {
                    moreBudget(credit);
                }
            }
        }

        @Override
        public void decBudget(long streamId, int amount)
        {
            assert budget - amount >= 0;
            budget -= amount;

            GroupStreamBudget streamBudget = streamMap.get(streamId);
            if (streamBudget != null)
            {
                streamBudget.unackedBudget += amount;
            }
        }

        @Override
        public int getBudget()
        {
            return budget;
        }

        @Override
        public void incBudget(long streamId, int credit, Runnable budgetAvailable)
        {
            GroupStreamBudget streamBudget = streamMap.get(streamId);

            if (streamBudget == null)
            {
                // Ignore initial window of a stream (except the very first stream)
                if (initialBudget == -1)
                {
                    // very first stream
                    initialBudget = credit;
                }
                else
                {
                    credit = 0;
                }
                streamBudget = new GroupStreamBudget(streamId, budgetAvailable);
                streamMap.put(streamId, streamBudget);
            }
            else
            {
                streamBudget.unackedBudget -= credit;
                if (streamBudget.unackedBudget < 0)
                {
                    streamBudget.unackedBudget = 0;
                }
                assert streamBudget.unackedBudget >= 0;
            }

            moreBudget(credit);
        }

        @Override
        public boolean hasUnackedBudget(long streamId)
        {
            if (groupId == 0)
            {
                return false;
            }
            else
            {
                GroupBudget groupBudget = groups.get(groupId);
                GroupStreamBudget streamBudget = groupBudget != null ? groupBudget.get(streamId) : null;
                return streamBudget != null && streamBudget.unackedBudget != 0;
            }
        }

        @Override
        public String toString()
        {
            long streams = streamMap.values().stream().count();
            long unackedStreams = streamMap.values().stream().filter(s -> s.unackedBudget > 0).count();

            return String.format("(groupId=%d budget=%d streams=%d unackedStreams=%d)",
                    groupId, budget, streams, unackedStreams);
        }
    }

    private static class StreamBudget implements Budget
    {
        private int budget;

        @Override
        public void decBudget(
            long streamId,
            int amount)
        {
            assert budget - amount >= 0;
            budget -= amount;
        }

        @Override
        public void closed(
            long streamId)
        {
        }

        @Override
        public void closing(
            long streamId,
            int credit)
        {
        }

        @Override
        public int getBudget()
        {
            return budget;
        }

        @Override
        public void incBudget(
            long streamId,
            int credit,
            Runnable budgetAvailable)
        {
            budget += credit;
            budgetAvailable.run();
        }

        @Override
        public boolean hasUnackedBudget(
            long streamId)
        {
            return false;
        }

        @Override
        public String toString()
        {
            return Integer.toString(budget);
        }
    }

    BudgetManager()
    {
        groups = new Long2ObjectHashMap<>();
    }

    Budget createBudget(long groupId)
    {
        Budget result;
        if (groupId == 0)
        {
            result = new StreamBudget();
        }
        else
        {
            result = groups.computeIfAbsent(groupId, GroupBudget::new);
        }
        return result;
    }
}
