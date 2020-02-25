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

import java.util.function.Function;

import org.reaktivity.nukleus.kafka.internal.cache.KafkaCachePartition.Node;

public final class KafkaCachePartitionView extends KafkaCacheObjects.ReadOnly
{
    private final KafkaCachePartition partition;
    private final NodeView sentinel;

    public KafkaCachePartitionView(
        KafkaCachePartition partition)
    {
        super(partition);
        this.partition = partition;
        this.sentinel = partition.sentinel().acquire(NodeView::new);
    }

    public String name()
    {
        return partition.name();
    }

    public int id()
    {
        return partition.id();
    }

    public NodeView sentinel()
    {
        return sentinel;
    }

    public NodeView seekNotBefore(
        long offset)
    {
        NodeView node = sentinel.next();

        while (node != sentinel && node.segment.baseOffset() < offset)
        {
            node = node.next();
        }

        return node;
    }

    public NodeView seekNotAfter(
        long offset)
    {
        NodeView node = sentinel.previous();

        while (node != sentinel && node.segment.baseOffset() > offset)
        {
            node = node.previous;
        }

        return node;
    }

    @Override
    public String toString()
    {
        return String.format("[%s] %s[%d]", getClass().getSimpleName(), name(), id());
    }

    @Override
    protected void onClosed()
    {
        NodeView node = sentinel.next();

        while (node != sentinel)
        {
            node.close();
            node = node.next();
        }

        node.close();
    }

    public final class NodeView extends KafkaCacheObjects.ReadOnly
    {
        private final KafkaCachePartition.Node node;
        private final KafkaCacheSegmentView segment;

        private NodeView previous;
        private NodeView next;
        private NodeView replacement;

        NodeView(
            KafkaCachePartition.Node node)
        {
            super(node);
            this.node = node;
            this.segment = node.segment() != null ? node.segment().acquire(KafkaCacheSegmentView::new) : null;
            this.previous = sentinel != null ? sentinel : this;
            this.next = sentinel != null ? sentinel : this;
        }

        public KafkaCacheSegmentView segment()
        {
            return segment;
        }

        public boolean sentinel()
        {
            return this == sentinel;
        }

        public NodeView previous()
        {
            return previous;
        }

        public NodeView next()
        {
            if (next.node.sentinel())
            {
                KafkaCachePartition.Node nodeNext = node.next();
                if (!nodeNext.sentinel())
                {
                    NodeView newNode = nodeNext.acquire(NodeView::new);
                    newNode.previous = this;
                    newNode.next = next;
                    next.previous = newNode;

                    this.next = newNode;
                    assert next.segment != null;
                }
            }

            return next;
        }

        public NodeView replacement()
        {
            if (replacement == null)
            {
                final Node nodeReplacement = node.replacement();
                if (nodeReplacement != null)
                {
                    NodeView newReplacement = nodeReplacement.acquire(NodeView::new);
                    newReplacement.previous = previous;
                    newReplacement.next = next;
                    next.previous = newReplacement;
                    previous.next = newReplacement;

                    this.replacement = newReplacement;
                }
            }

            return replacement;
        }

        @Override
        public String toString()
        {
            Function<KafkaCacheSegmentView, String> baseOffset = s -> s != null ? Long.toString(s.baseOffset()) : "sentinel";
            return String.format("[%s] %s", getClass().getSimpleName(), baseOffset.apply(segment));
        }

        @Override
        protected void onClosed()
        {
            if (this != sentinel)
            {
                assert segment != null;
                segment.close();
            }
        }
    }
}
