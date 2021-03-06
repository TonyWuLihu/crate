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

package io.crate.protocols.postgres;

import io.crate.action.sql.SQLOperations;
import io.crate.metadata.settings.CrateSettings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.network.NetworkAddress;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.PortsRange;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

@Singleton
public class PostgresNetty extends AbstractLifecycleComponent {

    private final SQLOperations sqlOperations;
    private final NetworkService networkService;

    private final boolean enabled;
    private final String port;

    private ServerBootstrap bootstrap;
    private ExecutorService bossExecutor;
    private ExecutorService workerExecutor;

    private volatile List<Channel> serverChannels = new ArrayList<>();

    @Inject
    public PostgresNetty(Settings settings, SQLOperations sqlOperations, NetworkService networkService) {
        super(settings);
        this.sqlOperations = sqlOperations;
        this.networkService = networkService;

        enabled = CrateSettings.PSQL_ENABLED.extract(settings);
        port = CrateSettings.PSQL_PORT.extract(settings);
    }

    @Override
    protected void doStart() {
        if (!enabled) {
            return;
        }

        bossExecutor = Executors.newCachedThreadPool(daemonThreadFactory(settings, "postgres-netty-boss"));
        workerExecutor = Executors.newCachedThreadPool(daemonThreadFactory(settings, "postgres-netty-worker"));
        bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(bossExecutor, workerExecutor));
        bootstrap.setOption("child.tcpNoDelay", settings.getAsBoolean("tcp_no_delay", true));
        bootstrap.setOption("child.keepAlive", settings.getAsBoolean("tcp_keep_alive", true));

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline pipeline = Channels.pipeline();

                ConnectionContext connectionContext = new ConnectionContext(sqlOperations);
                pipeline.addLast("frame-decoder", connectionContext.decoder);
                pipeline.addLast("handler", connectionContext.handler);
                return pipeline;
            }
        });

        resolveBindAddress();
    }

    private void resolveBindAddress() {
        // Bind and start to accept incoming connections.
        try {
            InetAddress[] hostAddresses = networkService.resolveBindHostAddresses(null);
            for (InetAddress address : hostAddresses) {
                bindAddress(address);
            }
        } catch (IOException e) {
            throw new BindPostgresException("Failed to resolve binding network host", e);
        }
    }

    private InetSocketTransportAddress bindAddress(final InetAddress hostAddress) {
        PortsRange portsRange = new PortsRange(port);
        final AtomicReference<Exception> lastException = new AtomicReference<>();
        final AtomicReference<InetSocketAddress> boundSocket = new AtomicReference<>();
        boolean success = portsRange.iterate(new PortsRange.PortCallback() {
            @Override
            public boolean onPortNumber(int portNumber) {
                try {
                    synchronized (serverChannels) {
                        Channel channel = bootstrap.bind(new InetSocketAddress(hostAddress, portNumber));
                        serverChannels.add(channel);
                        boundSocket.set((InetSocketAddress) channel.getLocalAddress());
                    }
                } catch (Exception e) {
                    lastException.set(e);
                    return false;
                }
                return true;
            }
        });
        if (!success) {
            throw new BindPostgresException("Failed to bind to [" + port + "]", lastException.get());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Bound psql to address {{}}", NetworkAddress.format(boundSocket.get()));
        }
        return new InetSocketTransportAddress(boundSocket.get());
    }

    @Override
    protected void doStop() {
        synchronized (serverChannels) {
            if (serverChannels != null) {
                for (Channel channel : serverChannels) {
                    channel.close().awaitUninterruptibly();
                }
                serverChannels = null;
            }
        }

        if (bootstrap != null) {
            bootstrap.shutdown();
            workerExecutor.shutdown();
            bossExecutor.shutdown();

            try {
                workerExecutor.awaitTermination(2, TimeUnit.SECONDS);
                bossExecutor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    protected void doClose() {
    }
}
