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

import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaDeltaType.JSON_PATCH;
import static org.reaktivity.nukleus.kafka.internal.types.KafkaOffsetType.LIVE;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;

import org.junit.Before;
import org.junit.Test;

public class KafkaOptionsAdapterTest
{
    private Jsonb jsonb;

    @Before
    public void initJson()
    {
        JsonbConfig config = new JsonbConfig()
                .withAdapters(new KafkaOptionsAdapter());
        jsonb = JsonbBuilder.create(config);
    }

    @Test
    public void shouldReadOptions()
    {
        String text =
                "{" +
                    "\"merged\":" +
                    "[" +
                        "\"test\"" +
                    "]," +
                    "\"bootstrap\":" +
                    "[" +
                        "\"test\"" +
                    "]," +
                    "\"topics\":" +
                    "[" +
                        "{" +
                            "\"name\": \"test\"," +
                            "\"defaultOffset\": \"live\"," +
                            "\"deltaType\": \"json_patch\"" +
                        "}" +
                    "]" +
                "}";

        KafkaOptions options = jsonb.fromJson(text, KafkaOptions.class);

        assertThat(options, not(nullValue()));
        assertThat(options.merged, equalTo(singletonList("test")));
        assertThat(options.bootstrap, equalTo(singletonList("test")));
        assertThat(options.topics, equalTo(singletonList(new KafkaTopic("test", LIVE, JSON_PATCH))));
    }

    @Test
    public void shouldWriteOptions()
    {
        KafkaOptions options = new KafkaOptions(
                singletonList("test"),
                singletonList("test"),
                singletonList(new KafkaTopic("test", LIVE, JSON_PATCH)));

        String text = jsonb.toJson(options);

        assertThat(text, not(nullValue()));
        assertThat(text, equalTo("{\"merged\":[\"test\"]," + "\"bootstrap\":[\"test\"]," +
                "\"topics\":[{\"name\":\"test\",\"defaultOffset\":\"live\",\"deltaType\":\"json_patch\"}]}"));
    }
}
