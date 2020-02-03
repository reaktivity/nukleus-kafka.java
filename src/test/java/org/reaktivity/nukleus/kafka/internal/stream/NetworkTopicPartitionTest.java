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

import java.util.NavigableSet;
import java.util.TreeSet;

import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.kafka.internal.stream.NetworkConnectionPool.NetworkTopicPartition;

public final class NetworkTopicPartitionTest
{
    private final NavigableSet<NetworkTopicPartition> partitions = new TreeSet<>();

    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldFindFloor()
    {
        NetworkTopicPartition partition;
        partition = new NetworkTopicPartition();
        partition.id = 0;
        partition.offset = 6789;
        partitions.add(partition);
        partition = new NetworkTopicPartition();
        partition.id = 0;
        partition.offset = 142706;
        partitions.add(partition);

        partition = new NetworkTopicPartition();
        partition.id = 1;
        partition.offset = 3574;
        partitions.add(partition);
        partition = new NetworkTopicPartition();
        partition.id = 1;
        partition.offset = 168550;
        partitions.add(partition);

        partition = new NetworkTopicPartition();
        partition.id = 2;
        partition.offset = 16968;
        partitions.add(partition);
        partition = new NetworkTopicPartition();
        partition.id = 2;
        partition.offset = 0;
        partitions.add(partition);
        partition = new NetworkTopicPartition();
        partition.id = 2;
        partition.offset = 168162;
        partitions.add(partition);

        partition = new NetworkTopicPartition();
        partition.id = 3;
        partition.offset = 426618;
        partitions.add(partition);

        NetworkTopicPartition  candidate = new NetworkTopicPartition();
        candidate.id = 2;
        candidate.offset = 0;
        NetworkTopicPartition first = partitions.floor(candidate);

        assertEquals(2, first.id);
        assertEquals(0, first.offset);

        candidate.id = 1;
        candidate.offset = 3573;
        first = partitions.floor(candidate);


        assertEquals(0, first.id);
        assertEquals(142706, first.offset);
    }

}
