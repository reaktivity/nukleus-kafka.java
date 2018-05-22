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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.rules.RuleChain.outerRule;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.kaazing.k3po.junit.annotation.ScriptProperty;
import org.kaazing.k3po.junit.annotation.Specification;
import org.kaazing.k3po.junit.rules.K3poRule;
import org.reaktivity.nukleus.kafka.internal.KafkaController;
import org.reaktivity.reaktor.test.ReaktorRule;

public class ControllerIT
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
        .directory("target/nukleus-itests")
        .commandBufferCapacity(1024)
        .responseBufferCapacity(1024)
        .counterValuesBufferCapacity(1024)
        .controller("kafka"::equals);

    @Rule
    public final TestRule chain = outerRule(k3po).around(timeout).around(reaktor);

    @Test
    @Specification({
        "${route}/client/nukleus"
    })
    public void shouldRouteClient() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target", targetRef, null)
               .get();

        k3po.finish();
    }


    @Test
    @Specification({
        "${route}/client/nukleus",
        "${unroute}/client/nukleus"
    })
    public void shouldUnrouteClient() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        long sourceRef = reaktor.controller(KafkaController.class)
              .routeClient("source", 0L, "target", targetRef, null)
              .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(KafkaController.class)
               .unrouteClient("source", sourceRef, "target", targetRef, null)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeEx}/client/nukleus"
    })
    public void shouldRouteClientWithExtension() throws Exception
    {
        long targetRef = new Random().nextLong();
        String topicName = "test";

        k3po.start();

        reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target", targetRef, topicName)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.header/client/nukleus"
    })
    public void shouldRouteClientWithTopicAndHeaderCondition() throws Exception
    {
        long targetRef = new Random().nextLong();
        String topicName = "test";
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("header1", "match1");

        k3po.start();

        reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target", targetRef, topicName, headers)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.headers/client/nukleus"
    })
    public void shouldRouteClientWithTopicAndHeaderConditions() throws Exception
    {
        long targetRef = new Random().nextLong();
        String topicName = "test";
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("header1", "match1");
        headers.put("header2", "match2");

        k3po.start();

        reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target", targetRef, topicName, headers)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.headers/client/nukleus"
    })
    public void shouldRouteClientWithMultipleRoutesDifferingOnlyInHeaders() throws Exception
    {
        long targetRef = new Random().nextLong();
        String topicName = "test";
        Map<String, String> headers1 = new LinkedHashMap<>();
        Map<String, String> headers2 = new LinkedHashMap<>();
        headers1.put("header1", "match1");
        headers2.put("header1", "match2");

        k3po.start();

        Long applicationRouteRef = reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target", targetRef, topicName, headers1)
               .get();

        assertEquals(applicationRouteRef, reaktor.controller(KafkaController.class)
                .routeClient("source", applicationRouteRef, "target", targetRef, topicName, headers2)
                .get());

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.networks/client/nukleus"
    })
    public void shouldRouteClientWithMultipleRoutesDifferentNetworks() throws Exception
    {
        long targetRef1 = new Random().nextLong();
        long targetRef2 = new Random().nextLong();
        String topicName = "test";

        k3po.start();

        long applicationRouteRef1 = reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target1", targetRef1, topicName)
               .get();

        long applicationRouteRef2 = reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target2", targetRef2, topicName)
               .get();

        assertNotEquals(applicationRouteRef1, applicationRouteRef2);

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.topics/client/nukleus"
    })
    public void shouldRouteClientWithMultipleRoutesDifferentTopics() throws Exception
    {
        long targetRef = new Random().nextLong();

        k3po.start();

        Long applicationRouteRef = reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target", targetRef, "test1")
               .get();

        CompletableFuture<Long> result2 = reaktor.controller(KafkaController.class)
               .routeClient("source", applicationRouteRef, "target", targetRef, "test2");

        CompletableFuture<Long> result3 = reaktor.controller(KafkaController.class)
               .routeClient("source", applicationRouteRef, "target", targetRef, "test3");

        assertEquals(applicationRouteRef, result2.get());
        assertEquals(applicationRouteRef, result3.get());

        k3po.finish();
    }

    @Test
    @Specification({
        "${routeEx}/client/nukleus",
        "${unrouteEx}/client/nukleus"
    })
    public void shouldUnrouteClientWithExtension() throws Exception
    {
        long targetRef = new Random().nextLong();
        String topicName = "test";

        k3po.start();

        long sourceRef = reaktor.controller(KafkaController.class)
              .routeClient("source", 0L, "target", targetRef, topicName)
              .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(KafkaController.class)
               .unrouteClient("source", sourceRef, "target", targetRef, topicName)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.header/client/nukleus",
        "${control}/unroute.ext.header/client/nukleus"
    })
    public void shouldUnrouteClientWithHeader() throws Exception
    {
        long targetRef = new Random().nextLong();
        String topicName = "test";
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("header1", "match1");

        k3po.start();

        long sourceRef = reaktor.controller(KafkaController.class)
              .routeClient("source", 0L, "target", targetRef, topicName, headers)
              .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(KafkaController.class)
               .unrouteClient("source", sourceRef, "target", targetRef, topicName, headers)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.headers/client/nukleus",
        "${control}/unroute.ext.headers/client/nukleus"
    })
    public void shouldUnrouteClientWithHeaders() throws Exception
    {
        long targetRef = new Random().nextLong();
        String topicName = "test";
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put("header1", "match1");
        headers.put("header2", "match2");

        k3po.start();

        long sourceRef = reaktor.controller(KafkaController.class)
              .routeClient("source", 0L, "target", targetRef, topicName, headers)
              .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(KafkaController.class)
               .unrouteClient("source", sourceRef, "target", targetRef, topicName, headers)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.headers/client/nukleus",
        "${control}/unroute.ext.multiple.headers/client/nukleus"
    })
    public void shouldUnrouteClientWithMultipleRoutesDifferingOnlyInHeaders() throws Exception
    {
        long targetRef = new Random().nextLong();
        String topicName = "test";
        Map<String, String> headers1 = new LinkedHashMap<>();
        Map<String, String> headers2 = new LinkedHashMap<>();
        headers1.put("header1", "match1");
        headers2.put("header2", "match2");

        k3po.start();

        Long applicationRouteRef = reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target", targetRef, topicName, headers1)
               .get();

        assertEquals(applicationRouteRef, reaktor.controller(KafkaController.class)
                .routeClient("source", applicationRouteRef, "target", targetRef, topicName, headers2)
                .get());

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(KafkaController.class)
                .unrouteClient("source", applicationRouteRef, "target",  targetRef, topicName, headers1)
                .get();

        reaktor.controller(KafkaController.class)
                .unrouteClient("source", applicationRouteRef, "target",  targetRef, topicName, headers2)
                .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${control}/route.ext.multiple.networks/client/nukleus",
        "${control}/unroute.ext.multiple.networks/client/nukleus"
    })
    public void shouldUnrouteClientWithMultipleRoutesDifferentNetworks() throws Exception
    {
        long targetRef1 = new Random().nextLong();
        long targetRef2 = new Random().nextLong();
        String topicName = "test";

        k3po.start();

        long applicationRouteRef1 = reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target1", targetRef1, topicName)
               .get();

        long applicationRouteRef2 = reaktor.controller(KafkaController.class)
               .routeClient("source", 0L, "target2", targetRef2, topicName)
               .get();

        k3po.notifyBarrier("ROUTED_CLIENT");

        reaktor.controller(KafkaController.class)
               .unrouteClient("source", applicationRouteRef1, "target1",  targetRef1, topicName)
               .get();

        reaktor.controller(KafkaController.class)
               .unrouteClient("source", applicationRouteRef2, "target2",  targetRef2, topicName)
               .get();

        k3po.finish();
    }

    @Test
    @Specification({
        "${freeze}/nukleus"
    })
    @ScriptProperty("nameF00N \"kafka\"")
    public void shouldFreeze() throws Exception
    {
        k3po.start();

        reaktor.controller(KafkaController.class)
               .freeze()
               .get();

        k3po.finish();
    }
}
