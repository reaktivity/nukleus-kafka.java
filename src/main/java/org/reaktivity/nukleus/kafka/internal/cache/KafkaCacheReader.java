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

import java.util.LinkedHashMap;
import java.util.Map;

public final class KafkaCacheReader
{
    private final KafkaCacheSegmentSupplier supplySegment;
    private final Map<String, KafkaCacheClusterReader> clustersByName;

    KafkaCacheReader(
        KafkaCacheSegmentSupplier supplySegment)
    {
        this.supplySegment = supplySegment;
        this.clustersByName = new LinkedHashMap<>();
    }

    public KafkaCacheClusterReader supplyCluster(
        String clusterName)
    {
        return clustersByName.computeIfAbsent(clusterName, this::newCluster);
    }

    private KafkaCacheClusterReader newCluster(
        String clusterName)
    {
        return new KafkaCacheClusterReader(clusterName, supplySegment);
    }
}
