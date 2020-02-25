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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.nio.file.Path;

import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;

public class KafkaCacheSegmentViewTest
{
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void shouldDescribeObject() throws Exception
    {
        KafkaCacheTopicConfig config = new KafkaCacheTopicConfig(new KafkaConfiguration());
        Path location = tempFolder.getRoot().toPath();
        MutableDirectBuffer appendBuf = new UnsafeBuffer(ByteBuffer.allocate(0));

        try (KafkaCacheSegment segment = new KafkaCacheSegment(location, config, "test", 0, 1L, appendBuf);
                KafkaCacheSegmentView segmentView = segment.acquire(KafkaCacheSegmentView::new))
        {
            assertEquals("[KafkaCacheSegmentView] test[0] @ 1", segmentView.toString());
        }
    }
}
