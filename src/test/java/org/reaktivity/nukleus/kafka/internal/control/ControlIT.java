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
package org.reaktivity.nukleus.kafka.internal.control;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.rules.RuleChain.outerRule;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.kafka.internal.KafkaConfiguration;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControlIT
{
    private final K3poRule k3po = new K3poRule()
            .addScriptRoot("control", "org/reaktivity/specification/nukleus/kafka/control")
            .addScriptRoot("route", "org/reaktivity/specification/nukleus/kafka/control/route")
            .addScriptRoot("unroute", "org/reaktivity/specification/nukleus/kafka/control/unroute")
            .addScriptRoot("routeEx", "org/reaktivity/specification/nukleus/kafka/control/route.ext")
            .addScriptRoot("unrouteEx", "org/reaktivity/specification/nukleus/kafka/control/unroute.ext")
            .addScriptRoot("freeze", "org/reaktivity/specification/nukleus/control/freeze");

    private final TestRule timeout = new DisableOnDebug(new Timeout(5, SECONDS));

    private final ReaktorRule reaktor = new ReaktorRule()
            .nukleus("kafka"::equals)
            .directory("target/nukleus-itests")
            .commandBufferCapacity(1024)
            .responseBufferCapacity(1024)
            .counterValuesBufferCapacity(1024)
            .configure(KafkaConfiguration.TOPIC_BOOTSTRAP_ENABLED, "false");

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    @Test
    @Specification({
        "${route}/client/controller"
    })
    public void shouldRouteClient() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${route}/client/controller",
        "${unroute}/client/controller"
    })
    public void shouldUnrouteClient() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeEx}/client/controller"
    })
    public void shouldRouteClientWithExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.networks/client/controller"
    })
    public void shouldRouteClientWithMultipleRoutesDifferentNetworks() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.topics/client/controller"
    })
    public void shouldRouteClientWithMultipleRoutesDifferentTopics() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${routeEx}/client/controller",
        "${unrouteEx}/client/controller"
    })
    public void shouldUnrouteClientWithExtension() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.networks/client/controller",
        "${control}/unroute.ext.multiple.networks/client/controller"
    })
    public void shouldUnrouteClientMultipleRoutesDifferentNetworks() throws Exception
    {
        k3po.finish();
    }

    @Ignore("TODO: first unroute currently removes both routes, need a way to compare unroute and route extensions")
    @Test
    @Specification({
        "${control}/route.ext.multiple.topics/client/controller",
        "${control}/unroute.ext.multiple.topics/client/controller"
    })
    public void shouldUnrouteClientMultipleRoutesDifferentTopics() throws Exception
    {
        k3po.finish();
    }

    @Test
    @Specification({
        "${freeze}/controller",
    })
    @ScriptProperty("nameF00C \"kafka\"")
    public void shouldFreeze() throws Exception
    {
        k3po.finish();
    }
}
