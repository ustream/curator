/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.nodes;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CreateModable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>A Non-Destructive, Persistent Ephemeral Node</p>
 * <ul>
 *  <li>Non-Destructive, because it doesn't overwrite / delete an already existing node at the specified path</li>
 *  <li>Persistent, because it attempts to stay present in Zookeeper, even through connection and session interruptions</li>
 *  <li>Ephemeral, because it is created with {@link CreateMode#EPHEMERAL} and gets deleted after the specified session timeout</li>
 * </ul>
 * <p>Based on {@link PersistentNode}</p>
 * <p>Thanks to bbeck (https://github.com/bbeck) for the initial coding and design</p>
 */
public class NDPENode implements Closeable
{
    private final AtomicReference<InitialCreateLatch> initialCreateLatch = new AtomicReference<InitialCreateLatch>(new InitialCreateLatch());
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    private final CreateModable<ACLBackgroundPathAndBytesable<String>> createMethod;
    private final AtomicReference<String> nodePath = new AtomicReference<String>(null);
    private final String basePath;
    private final AtomicReference<byte[]> data = new AtomicReference<byte[]>();
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final BackgroundCallback backgroundCallback;
    private final AtomicBoolean allowedToOverwrite = new AtomicBoolean(false);
    private final CuratorWatcher watcher = new CuratorWatcher()
    {
        @Override
        public void process(WatchedEvent event) throws Exception
        {
            if ( event.getType() == EventType.NodeDeleted )
            {
                createNode();
            }
            else if ( event.getType() == EventType.NodeDataChanged )
            {
                watchNode();
            }
        }
    };
    private final BackgroundCallback checkExistsCallback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
            {
                createNode();
            }
            else
            {
                boolean isEphemeral = event.getStat().getEphemeralOwner() != 0;
                if ( !isEphemeral )
                {
                    log.warn("Existing node ephemeral state doesn't match requested state. Maybe the node was created outside of NDPENode? " + basePath);
                }
            }
        }
    };
    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            if ( newState == ConnectionState.RECONNECTED )
            {
                createNode();
            }
        }
    };

    private static class InitialCreateLatch
    {
        private final AtomicReference<CountDownLatch> countDownLatch = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        private final AtomicBoolean nodeExists = new AtomicBoolean(false);

        private void nodeExists()
        {
            nodeExists.set(true);
            countDown();
        }

        private void countDown()
        {
            CountDownLatch localLatch = countDownLatch.get();
            localLatch.countDown();
        }

        private CreateError await(final long timeout, final TimeUnit unit) throws InterruptedException
        {
            CountDownLatch localLatch = countDownLatch.get();
            final boolean success = localLatch.await(timeout, unit);
            if ( success )
            {
                return CreateError.NONE;
            }
            if ( nodeExists.get() )
            {
                return CreateError.NODE_EXISTS;
            }
            return CreateError.TIMEOUT;
        }
    }

    public enum CreateError
    {
        NONE,
        NODE_EXISTS,
        TIMEOUT
    }

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * @param client        client instance
     * @param basePath the base path for the node
     * @param initData data for the node
     */
    public NDPENode(CuratorFramework client, final String basePath, byte[] initData)
    {
        this.client = Preconditions.checkNotNull(client, "client cannot be null");
        this.basePath = PathUtils.validatePath(basePath);
        final byte[] data = Preconditions.checkNotNull(initData, "data cannot be null");

        backgroundCallback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( state.get() == State.STARTED )
                {
                    processBackgroundCallback(event);
                }
                else
                {
                    processBackgroundCallbackClosedState(event);
                }
            }
        };

        createMethod = client.create().creatingParentContainersIfNeeded();
        this.data.set(Arrays.copyOf(data, data.length));
    }

    private void processBackgroundCallbackClosedState(CuratorEvent event)
    {
        String path = null;
        if ( event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue() )
        {
            path = event.getPath();
        }
        else if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
        {
            path = event.getName();
        }

        if ( path != null && allowedToOverwrite.get() )
        {
            try
            {
                client.delete().guaranteed().inBackground().forPath(path);
            }
            catch ( Exception e )
            {
                log.error("Could not delete node after close", e);
            }
        }
    }

    private void processBackgroundCallback(CuratorEvent event) throws Exception
    {
        if ( event.getResultCode() == KeeperException.Code.NODEEXISTS.intValue() )
        {
            InitialCreateLatch localLatch = initialCreateLatch.get();
            if ( localLatch != null )
            {
                localLatch.nodeExists();
            }
            return;
        }

        if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
        {
            String path = event.getName();
            nodePath.set(path);
            allowedToOverwrite.set(true);
            watchNode();

            initialisationComplete();
            return;
        }

        createNode();
    }

    private void initialisationComplete()
    {
        InitialCreateLatch localLatch = initialCreateLatch.getAndSet(null);
        if ( localLatch != null )
        {
            localLatch.countDown();
        }
    }

    /**
     * You must call start() to initiate the persistent node. An attempt to create the node
     * in the background will be started
     */
    public void start()
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Already started");

        client.getConnectionStateListenable().addListener(connectionStateListener);
        createNode();
    }

    /**
     * Block until initial node creation initiated by {@link #start()} either
     * <ul>
     *     <li>succeeds, or</li>
     *     <li>fails because the node at the specified path already exists, or</li>
     *     <li>the timeout elapses.</li>
     * </ul>
     *
     * @param timeout the maximum time to wait
     * @param unit    time unit
     * @return {@link CreateError#NONE} if the node was created before timeout
     *         {@link CreateError#NODE_EXISTS} if the node was not created because a node already exists at the specified path
     *         {@link CreateError#TIMEOUT} if the node was not created before timeout for any other reason
     * @throws InterruptedException if the thread is interrupted
     */
    public CreateError waitForInitialCreate(long timeout, TimeUnit unit) throws InterruptedException
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");

        InitialCreateLatch localLatch = initialCreateLatch.get();
        return localLatch == null ? CreateError.NONE : localLatch.await(timeout, unit);
    }

    @Override
    public void close() throws IOException
    {
        if ( !state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            return;
        }

        client.getConnectionStateListenable().removeListener(connectionStateListener);

        if ( allowedToOverwrite.get() )
        {
            try
            {
                deleteNode();
            }
            catch (Exception e)
            {
                ThreadUtils.checkInterrupted(e);
                throw new IOException(e);
            }
        }
    }

    private void deleteNode() throws Exception
    {
        String localNodePath = nodePath.getAndSet(null);
        if ( localNodePath != null )
        {
            try
            {
                client.delete().guaranteed().forPath(localNodePath);
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // ignore
            }
        }
    }

    private void createNode()
    {
        if ( !isActive() )
        {
            return;
        }

        try
        {
            String existingPath = nodePath.get();
            String createPath = existingPath != null ? existingPath : basePath;
            createMethod.withMode(CreateMode.EPHEMERAL).inBackground(backgroundCallback).forPath(createPath, data.get());
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new RuntimeException("Creating node. BasePath: " + basePath, e);  // should never happen unless there's a programming error - so throw RuntimeException
        }
    }

    private void watchNode() throws Exception
    {
        if ( !isActive() )
        {
            return;
        }

        String localNodePath = nodePath.get();
        if ( localNodePath != null )
        {
            client.checkExists().usingWatcher(watcher).inBackground(checkExistsCallback).forPath(localNodePath);
        }
    }

    private boolean isActive()
    {
        return (state.get() == State.STARTED);
    }
}
