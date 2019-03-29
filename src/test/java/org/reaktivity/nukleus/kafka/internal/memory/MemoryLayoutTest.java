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
package org.reaktivity.nukleus.kafka.internal.memory;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.nio.file.Path;

import org.junit.Test;

public class MemoryLayoutTest
{
    private static final int GB_1 = 1024 * 1024 * 1024;
    private static final long GB_2 = 2L * GB_1;
    private static final int BYTES_64 = 64;

    final Path outputFile = new File("target/nukleus-itests/memory0").toPath();
    MemoryLayout.Builder builder = new MemoryLayout.Builder()
            .path(outputFile);

    @Test
    public void shouldCreateLayoutWithCapacityGreaterThanOneGB()
    {
        MemoryLayout layout = builder
               .create(true)
               .minimumBlockSize(BYTES_64)
               .capacity(GB_2)
               .build();
        assertEquals(2, layout.memoryBuffers().length);
        assertEquals(GB_1, layout.memoryBuffers()[0].capacity());
        assertEquals(GB_1, layout.memoryBuffers()[1].capacity());
        assertEquals(BYTES_64, layout.minimumBlockSize());
        assertEquals(GB_2, layout.capacity());
    }

    @Test
    public void shouldCreateLayoutWithCapacityOneGB()
    {
        MemoryLayout layout = builder
               .create(true)
               .minimumBlockSize(BYTES_64)
               .capacity(GB_1)
               .build();
        assertEquals(1, layout.memoryBuffers().length);
        assertEquals(GB_1, layout.memoryBuffers()[0].capacity());
    }
}
