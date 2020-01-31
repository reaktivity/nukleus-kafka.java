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
package org.reaktivity.nukleus.kafka.internal.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.reaktivity.nukleus.kafka.internal.KafkaNukleus;

public final class KafkaCacheWriter
{
    public static final String TYPE_NAME = String.format("%s/cache", KafkaNukleus.NAME);

    private final KafkaCacheSegmentSupplier segmentSupplier;
    private final Map<String, KafkaCacheClusterWriter> clustersByName;

    KafkaCacheWriter(
        KafkaCacheSegmentSupplier segmentSupplier)
    {
        this.segmentSupplier = segmentSupplier;
        this.clustersByName = new ConcurrentHashMap<>();
    }

    public KafkaCacheClusterWriter supplyCluster(
        String clusterName)
    {
        return clustersByName.computeIfAbsent(clusterName, this::newCluster);
    }

    private KafkaCacheClusterWriter newCluster(
        String clusterName)
    {
        return new KafkaCacheClusterWriter(clusterName, segmentSupplier);
    }
}
