/**
 * Copyright 2016-2017 The Reaktivity Project
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

import org.agrona.collections.Long2LongHashMap;

interface PartitionOffsets
{
    @FunctionalInterface
    interface Visitor
    {
        void visit(int partitionId, long offset);
    }

    void visit(Visitor visit);

    class SinglePartitionOffset implements PartitionOffsets
    {
        private final long offset;
        int partitionId;

        SinglePartitionOffset(long offset)
        {
            this.offset = offset;
        }

        @Override
        public void visit(
            Visitor visitor)
        {
            visitor.visit(partitionId, offset);
        }
    }

    class MultiplePartitionOffsets implements PartitionOffsets
    {
        final long[] offsets;

        MultiplePartitionOffsets(Long2LongHashMap fetchOffsets)
        {
            this.offsets = new long[fetchOffsets.size()];
            for (int i=0; i < fetchOffsets.size(); i++)
            {
                offsets[i] = fetchOffsets.get(i);
            }
        }

        @Override
        public void visit(
            Visitor visit)
        {
            // TODO Auto-generated method stub
        }

    }
}
