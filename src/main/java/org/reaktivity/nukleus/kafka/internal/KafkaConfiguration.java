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
package org.reaktivity.nukleus.kafka.internal;

import org.reaktivity.nukleus.Configuration;

public class KafkaConfiguration extends Configuration
{

    public static final String TOPIC_BOOTSTRAP_ENABLED = "nukleus.kafka.enable.topic.bootstrap";

    /**
     * @see java.nio.channels.ServerSocketChannel#bind(java.net.SocketAddress, int)
     */
    private static final boolean TOPIC_BOOTSTRAP_ENABLED_DEFAULT = false;

    public KafkaConfiguration(
        Configuration config)
    {
        super(config);
    }

    public boolean bootstrapEnabled()
    {
        return getBoolean(TOPIC_BOOTSTRAP_ENABLED, TOPIC_BOOTSTRAP_ENABLED_DEFAULT);
    }
}
