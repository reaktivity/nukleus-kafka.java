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

import static java.lang.String.format;

public enum KafkaError
{
    NONE((short) 0),
    OFFSET_OUT_OF_RANGE((short) 1),
    UNKNOWN_TOPIC_OR_PARTITION((short) 3),
    LEADER_NOT_AVAILABLE((short) 5),
    NOT_LEADER_FOR_PARTITION((short) 6),
    INVALID_TOPIC_EXCEPTION((short) 17),
    TOPIC_AUTHORIZATION_FAILED((short) 29),
    UNEXPECTED_SERVER_ERROR((short) -1),
    UNRECOGNIZED_ERROR_CODE((short) -2),
    PARTITION_COUNT_CHANGED((short) -3);

    boolean isRecoverable()
    {
        return errorCode == LEADER_NOT_AVAILABLE.errorCode;
    }

    final short errorCode;

    KafkaError(short errorCode)
    {
        this.errorCode = errorCode;
    }

    @Override
    public String toString()
    {
        return format("%d (%s)", errorCode, name());
    }

    public static KafkaError asKafkaError(short errorCode)
    {
        KafkaError result = UNRECOGNIZED_ERROR_CODE;
        KafkaError[] values = KafkaError.values();
        for (int i=0; i < values.length; i++)
        {
            if (values[i].errorCode == errorCode)
            {
                result = values[i];
                break;
            }
        }
        return result;
    }
}
