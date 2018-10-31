/**
 * Copyright 2016-2018 The Reaktivity Project
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

public interface MemoryManager
{
    long OUT_OF_MEMORY = -1L;

    /**
     * Allocates memory and returns the relative address
     * @param capacity    Number of bytes of memory to allocate
     * @return            Address, or OUT_OF_MEMORY if not enough memory is available
     */
    long acquire(int capacity);

    /**
     * Converts a relative address into an absolute address
     * @param address  Relative address
     * @return         Absolute memory address
     */
    long resolve(long address);

    /**
     * Releases a previously allocated area of memory
     * @param address     Address previously allocated using acquire
     * @param capacity    Number of bytes of memory previously allocated using acquire
     */
    void release(long address, int capacity);
}
