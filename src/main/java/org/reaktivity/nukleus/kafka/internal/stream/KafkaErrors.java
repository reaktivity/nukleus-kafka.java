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

public class KafkaErrors
{
    static final short NONE = 0;
    static final short OFFSET_OUT_OF_RANGE = 1;
    public static final short UNKNOWN_TOPIC_OR_PARTITION = 3;
    static final short LEADER_NOT_AVAILABLE = 5;
    static final short NOT_LEADER_FOR_PARTITION = 6;
    static final short INVALID_TOPIC_EXCEPTION = 17;
    static final short TOPIC_AUTHORIZATION_FAILED = 29;
    static final short UNKNOWN = -1;

    static boolean isRecoverable(short errorCode)
    {
        return errorCode == LEADER_NOT_AVAILABLE;
    }
}
