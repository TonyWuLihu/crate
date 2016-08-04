/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.collect.collectors;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.executor.transport.NodeStatsRequest;
import io.crate.executor.transport.NodeStatsResponse;
import io.crate.executor.transport.TransportNodeStatsAction;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowCollectExpression;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.projectors.IterableRowEmitter;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.node.NodeStatsContext;
import io.crate.planner.node.dql.RoutedCollectPhase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeStatsCollector implements CrateCollector {

    private final TransportNodeStatsAction transportStatTablesAction;
    private final RowReceiver rowReceiver;
    private final RoutedCollectPhase collectPhase;
    private final DiscoveryNodes nodes;
    private final CollectInputSymbolVisitor<RowCollectExpression<?, ?>> inputSymbolVisitor;
    private final ReferenceIdentVisitor referenceIdentVisitor = ReferenceIdentVisitor.INSTANCE;
    private final AtomicInteger remainingRequests = new AtomicInteger();

    public NodeStatsCollector(TransportNodeStatsAction transportStatTablesAction,
                              RowReceiver rowReceiver,
                              RoutedCollectPhase collectPhase,
                              DiscoveryNodes nodes,
                              CollectInputSymbolVisitor<RowCollectExpression<?, ?>> inputSymbolVisitor) {
        this.transportStatTablesAction = transportStatTablesAction;
        this.rowReceiver = rowReceiver;
        this.collectPhase = collectPhase;
        this.nodes = nodes;
        this.inputSymbolVisitor = inputSymbolVisitor;
    }

    @Override
    public void doCollect() {
        remainingRequests.set(nodes.size());
        final List<NodeStatsContext> rows = Collections.synchronizedList(new ArrayList<NodeStatsContext>());
        for (final DiscoveryNode node : nodes) {

            SettableFuture<NodeStatsContext> future = fetchRowFromNode(node, SettableFuture.<NodeStatsContext>create());
            Futures.addCallback(future, new FutureCallback<NodeStatsContext>() {

                @Override
                public void onSuccess(NodeStatsContext context) {
                    rows.add(context);
                    if (remainingRequests.decrementAndGet() == 0) {
                        emmitRows(rows);
                    }
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    if (t instanceof ReceiveTimeoutTransportException) {
                        rows.add(new NodeStatsContext(node.id(), node.name()));
                        if (remainingRequests.decrementAndGet() == 0) {
                            emmitRows(rows);
                        }
                    } else {
                        rowReceiver.fail(t);
                    }
                }
            });
        }
    }

    private SettableFuture<NodeStatsContext> fetchRowFromNode(final DiscoveryNode node,
                                                              final SettableFuture<NodeStatsContext> future) {
        List<ReferenceIdent> toCollect = referenceIdentVisitor.process(collectPhase.toCollect());
        final NodeStatsRequest request = new NodeStatsRequest(node.id(), toCollect);

        transportStatTablesAction.execute(node.id(), request, new ActionListener<NodeStatsResponse>() {

            @Override
            public void onResponse(NodeStatsResponse response) {
                future.set(response.nodeStatsContext());
            }

            @Override
            public void onFailure(Throwable t) {
                future.setException(t);
            }
        }, TimeValue.timeValueMillis(3000L));
        return future;
    }

    private void emmitRows(List<NodeStatsContext> rows) {
        new IterableRowEmitter(
            rowReceiver,
            RowsTransformer.toRowsIterable(inputSymbolVisitor, collectPhase, rows)
        ).run();
    }

    @Override
    public void kill(@Nullable Throwable throwable) {
    }

    private static class ReferenceIdentVisitor extends DefaultTraversalSymbolVisitor<ReferenceIdentVisitor.Context, Void> {

        static final ReferenceIdentVisitor INSTANCE = new ReferenceIdentVisitor();

        static class Context {

            private final List<ReferenceIdent> referenceIdents = new ArrayList<>();

            void add(ReferenceIdent referenceIdent) {
                referenceIdents.add(referenceIdent);
            }

            List<ReferenceIdent> referenceIdents() {
                return referenceIdents;
            }
        }

        List<ReferenceIdent> process(Collection<? extends Symbol> symbols) {
            Context context = new Context();
            for (Symbol symbol : symbols) {
                process(symbol, context);
            }
            return context.referenceIdents();
        }

        @Override
        public Void visitReference(Reference symbol, Context context) {
            context.add(symbol.ident());
            return null;
        }
    }
}
