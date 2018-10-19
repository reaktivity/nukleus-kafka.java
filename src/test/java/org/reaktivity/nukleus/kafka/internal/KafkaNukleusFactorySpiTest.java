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
package org.reaktivity.nukleus.kafka.internal;

import static org.junit.Assert.assertNotNull;
import static org.reaktivity.nukleus.route.RouteKind.CLIENT;

import java.util.Properties;
import java.util.function.Predicate;

import org.jmock.Expectations;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.junit.Rule;
import org.junit.Test;
import org.reaktivity.nukleus.Configuration;
import org.reaktivity.nukleus.Nukleus;
import org.reaktivity.nukleus.NukleusBuilder;
import org.reaktivity.nukleus.NukleusFactory;
import org.reaktivity.nukleus.stream.StreamFactoryBuilder;
import org.reaktivity.reaktor.internal.ReaktorConfiguration;

public class KafkaNukleusFactorySpiTest
{
    @Rule
    public JUnitRuleMockery context = new JUnitRuleMockery();

    @Test
    public void shouldCreateKafkaNukleus()
    {
        final NukleusBuilder builder = context.mock(NukleusBuilder.class, "builder");

        context.checking(new Expectations()
        {
            {
                oneOf(builder).streamFactory(with(CLIENT), with(any(StreamFactoryBuilder.class)));
            }
        });

        Properties properties = new Properties();
        properties.setProperty(ReaktorConfiguration.DIRECTORY_PROPERTY_NAME, "target/nukleus-tests");
        Configuration config = new Configuration(properties);

        NukleusFactory factory = NukleusFactory.instantiate();
        Nukleus nukleus = factory.create("kafka", config, builder);

        assertNotNull(nukleus);

        Predicate<?> test = "create"::equals;
        test = test.or("delete"::equals);
    }

}
