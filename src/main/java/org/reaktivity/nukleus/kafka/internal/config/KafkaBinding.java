/**
 * Copyright 2016-2021 The Reaktivity Project
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
package org.reaktivity.nukleus.kafka.internal.config;

import static java.util.stream.Collectors.toList;

import java.util.List;

import org.reaktivity.reaktor.config.Binding;
import org.reaktivity.reaktor.config.Role;

public final class KafkaBinding
{
    public final long id;
    public final long vaultId;
    public final String entry;
    public final KafkaOptions options;
    public final Role kind;
    public final List<KafkaRoute> routes;
    public final KafkaRoute exit;

    public KafkaBinding(
        Binding binding)
    {
        this.id = binding.id;
        this.vaultId = binding.vault != null ? binding.vault.id : 0L;
        this.entry = binding.entry;
        this.kind = binding.kind;
        this.options = KafkaOptions.class.cast(binding.options);
        this.routes = binding.routes.stream().map(KafkaRoute::new).collect(toList());
        this.exit = binding.exit != null ? new KafkaRoute(binding.exit) : null;
    }

    public KafkaRoute resolve(
        long authorization,
        String topic)
    {
        return routes.stream()
            .filter(r -> r.when.stream().allMatch(m -> m.matches(topic)))
            .findFirst()
            .orElse(exit);
    }

    public KafkaTopic topic(
        String topic)
    {
        return topic != null &&
                options != null &&
                options.topics != null
                    ? options.topics.stream().filter(t -> topic.equals(t.name)).findFirst().orElse(null)
                    : null;
    }

    public boolean bootstrap(
        String topic)
    {
        return topic != null &&
                options != null &&
                options.bootstrap != null &&
                options.bootstrap.contains(topic);
    }

    public boolean merged(
        String topic)
    {
        return topic != null &&
                options != null &&
                options.merged != null &&
                options.merged.contains(topic);
    }
}
