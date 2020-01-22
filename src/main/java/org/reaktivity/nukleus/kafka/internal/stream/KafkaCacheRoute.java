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
package org.reaktivity.nukleus.kafka.internal.stream;

import java.util.Map;
import java.util.TreeMap;

import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheClientFetchFactory.KafkaCacheClientFetchFanout;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheDescribeFactory.KafkaCacheDescribeFanout;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheMetaFactory.KafkaCacheMetaFanout;
import org.reaktivity.nukleus.kafka.internal.stream.KafkaCacheServerFetchFactory.KafkaCacheServerFetchFanout;

public final class KafkaCacheRoute
{
    public final long routeId;
    public final Map<String, KafkaCacheDescribeFanout> describeFanoutsByTopic;
    public final Map<String, KafkaCacheMetaFanout> metaFanoutsByTopic;
    public final Map<String, KafkaCacheClientFetchFanout> clientFetchFanoutsByTopic;
    public final Map<String, KafkaCacheServerFetchFanout> serverFetchFanoutsByTopic;

    public KafkaCacheRoute(
        long routeId)
    {
        this.routeId = routeId;
        this.describeFanoutsByTopic = new TreeMap<>();
        this.metaFanoutsByTopic = new TreeMap<>();
        this.clientFetchFanoutsByTopic = new TreeMap<>();
        this.serverFetchFanoutsByTopic = new TreeMap<>();
    }
}
