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

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.BaseClassForTests;
import org.apache.curator.test.KillSession;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestNDPENode extends BaseClassForTests
{
    private static final String DIR = "/test";
    private static final String PATH = ZKPaths.makePath(DIR, "/foo");

    private final Collection<CuratorFramework> curatorInstances = Lists.newArrayList();
    private final Collection<NDPENode> createdNodes = Lists.newArrayList();

    private final Timing timing = new Timing();

    @AfterMethod
    public void teardown() throws Exception
    {
        for ( NDPENode node : createdNodes )
        {
            node.close();
        }

        for ( CuratorFramework curator : curatorInstances )
        {
            curator.close();
        }

        super.teardown();
    }

    @Test
    public void testListenersReconnectedIsFast() throws Exception
    {
        server.stop();

        CuratorFramework client = CuratorFrameworkFactory
                .newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            NDPENode node = new NDPENode(client, "/abc/node", "hello".getBytes());
            node.start();

            final CountDownLatch connectedLatch = new CountDownLatch(1);
            final CountDownLatch reconnectedLatch = new CountDownLatch(1);
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.CONNECTED )
                    {
                        connectedLatch.countDown();
                    }
                    if ( newState == ConnectionState.RECONNECTED )
                    {
                        reconnectedLatch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);
            timing.sleepABit();
            server.restart();
            Assert.assertTrue(timing.awaitLatch(connectedLatch));
            timing.sleepABit();
            Assert.assertEquals(node.waitForInitialCreate(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), NDPENode.CreateError.NONE);
            server.stop();
            timing.sleepABit();
            server.restart();
            timing.sleepABit();
            Assert.assertTrue(timing.awaitLatch(reconnectedLatch));
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNoServerAtStart() throws Exception
    {
        server.stop();

        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        try
        {
            client.start();
            NDPENode node = new NDPENode(client, "/abc/node", "hello".getBytes());
            node.start();

            final CountDownLatch connectedLatch = new CountDownLatch(1);
            ConnectionStateListener listener = new ConnectionStateListener()
            {
                @Override
                public void stateChanged(CuratorFramework client, ConnectionState newState)
                {
                    if ( newState == ConnectionState.CONNECTED )
                    {
                        connectedLatch.countDown();
                    }
                }
            };
            client.getConnectionStateListenable().addListener(listener);

            timing.sleepABit();

            server.restart();

            Assert.assertTrue(timing.awaitLatch(connectedLatch));

            timing.sleepABit();

            Assert.assertEquals(node.waitForInitialCreate(timing.forWaiting().milliseconds(), TimeUnit.MILLISECONDS), NDPENode.CreateError.NONE);
        }
        finally
        {
            CloseableUtils.closeQuietly(client);
        }
    }

    @Test
    public void testNoServer() throws Exception
    {
        server.stop();

        CuratorFramework client = newCurator();
        NDPENode node = new NDPENode(client, "/abc/node", "hello".getBytes());
        node.start();
        Assert.assertEquals(node.waitForInitialCreate(1L, TimeUnit.SECONDS), NDPENode.CreateError.TIMEOUT);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullCurator() throws Exception
    {
        new NDPENode(null, PATH, new byte[0]);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testNullPath() throws Exception
    {
        CuratorFramework curator = newCurator();
        new NDPENode(curator, null, new byte[0]);
    }

    @Test(expectedExceptions = NullPointerException.class)
    public void testNullData() throws Exception
    {
        CuratorFramework curator = newCurator();
        new NDPENode(curator, PATH, null);
    }

    @Test
    public void testDeletesNodeWhenClosed() throws Exception
    {
        CuratorFramework curator = newCurator();

        NDPENode node = new NDPENode(curator, PATH, new byte[0]);
        node.start();
        try
        {
            node.waitForInitialCreate(5, TimeUnit.SECONDS);
            assertNodeExists(curator, PATH);
        }
        finally
        {
            node.close();  // After closing the path is set to null...
        }

        assertNodeDoesNotExist(curator, PATH);
    }

    @Test
    public void testClosingMultipleTimes() throws Exception
    {
        CuratorFramework curator = newCurator();

        NDPENode node = new NDPENode(curator, PATH, new byte[0]);
        node.start();
        node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);

        node.close();
        assertNodeDoesNotExist(curator, PATH);

        node.close();
        assertNodeDoesNotExist(curator, PATH);
    }

    @Test
    public void testDeletesNodeWhenSessionDisconnects() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        NDPENode node = new NDPENode(curator, PATH, new byte[0]);
        node.start();
        try
        {
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertNodeExists(observer, PATH);

            // Register a watch that will fire when the node is deleted...
            Trigger deletedTrigger = Trigger.deleted();
            observer.checkExists().usingWatcher(deletedTrigger).forPath(PATH);

            killSession(curator);

            // Make sure the node got deleted
            assertTrue(deletedTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            node.close();
        }
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnects() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        NDPENode node = new NDPENode(curator, PATH, new byte[0]);
        node.start();
        try
        {
            node.waitForInitialCreate(5, TimeUnit.SECONDS);
            assertNodeExists(observer, PATH);

            Trigger deletedTrigger = Trigger.deleted();
            observer.checkExists().usingWatcher(deletedTrigger).forPath(PATH);

            killSession(curator);

            // Make sure the node got deleted...
            assertTrue(deletedTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));

            // Check for it to be recreated...
            Trigger createdTrigger = Trigger.created();
            Stat stat = observer.checkExists().usingWatcher(createdTrigger).forPath(PATH);
            assertTrue(stat != null || createdTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            node.close();
        }
    }

    @Test
    public void testRecreatesNodeWhenSessionReconnectsMultipleTimes() throws Exception
    {
        CuratorFramework curator = newCurator();
        CuratorFramework observer = newCurator();

        NDPENode node = new NDPENode(curator, PATH, new byte[0]);
        node.start();
        try
        {
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertNodeExists(observer, PATH);

            // We should be able to disconnect multiple times and each time the node should be recreated.
            for ( int i = 0; i < 5; i++ )
            {
                Trigger deletionTrigger = Trigger.deleted();
                observer.checkExists().usingWatcher(deletionTrigger).forPath(PATH);

                // Kill the session, thus cleaning up the node...
                killSession(curator);

                // Make sure the node ended up getting deleted...
                assertTrue(deletionTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));

                // Now put a watch in the background looking to see if it gets created...
                Trigger creationTrigger = Trigger.created();
                Stat stat = observer.checkExists().usingWatcher(creationTrigger).forPath(PATH);
                assertTrue(stat != null || creationTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
            }
        }
        finally
        {
            node.close();
        }
    }

    @Test
    public void testRecreatesNodeWhenItGetsDeleted() throws Exception
    {
        CuratorFramework curator = newCurator();

        NDPENode node = new NDPENode(curator, PATH, new byte[0]);
        node.start();
        try
        {
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertNodeExists(curator, PATH);

            // Delete the original node...
            curator.delete().forPath(PATH);

            // Since we're using an ephemeral node, and the original session hasn't been interrupted the name of the new
            // node that gets created is going to be exactly the same as the original.
            Trigger createdWatchTrigger = Trigger.created();
            Stat stat = curator.checkExists().usingWatcher(createdWatchTrigger).forPath(PATH);
            assertTrue(stat != null || createdWatchTrigger.firedWithin(timing.forWaiting().seconds(), TimeUnit.SECONDS));
        }
        finally
        {
            node.close();
        }
    }

    @Test
    public void testData() throws Exception
    {
        CuratorFramework curator = newCurator();
        byte[] data = "Hello World".getBytes();

        NDPENode node = new NDPENode(curator, PATH, data);
        node.start();
        try
        {
            node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS);
            assertTrue(Arrays.equals(curator.getData().forPath(PATH), data));
        }
        finally
        {
            node.close();
        }
    }

    @Test
    public void testDontOverwriteOrDeleteExisting() throws Exception
    {
        final byte[] TEST_DATA = "hey".getBytes();
        final byte[] ALT_TEST_DATA = "there".getBytes();

        CuratorFramework curator = newCurator();
        NDPENode node = new NDPENode(curator, PATH, ALT_TEST_DATA);
        try
        {
            curator.create().creatingParentsIfNeeded().forPath(PATH, TEST_DATA);
            node.start();
            Assert.assertEquals(node.waitForInitialCreate(timing.forWaiting().seconds(), TimeUnit.SECONDS), NDPENode.CreateError.NODE_EXISTS);
            Assert.assertEquals(curator.getData().forPath(PATH), TEST_DATA);
            node.close();
            timing.sleepABit();
            Assert.assertEquals(curator.getData().forPath(PATH), TEST_DATA);
        }
        finally
        {
            CloseableUtils.closeQuietly(node);
        }
    }

    private void assertNodeExists(CuratorFramework curator, String path) throws Exception
    {
        assertNotNull(path);
        assertTrue(curator.checkExists().forPath(path) != null);
    }

    private void assertNodeDoesNotExist(CuratorFramework curator, String path) throws Exception
    {
        assertTrue(curator.checkExists().forPath(path) == null);
    }

    private CuratorFramework newCurator() throws IOException
    {
        CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), timing.session(), timing.connection(), new RetryOneTime(1));
        client.start();

        curatorInstances.add(client);
        return client;
    }

    public void killSession(CuratorFramework curator) throws Exception
    {
        KillSession.kill(curator.getZookeeperClient().getZooKeeper(), curator.getZookeeperClient().getCurrentConnectionString());
    }

    private static final class Trigger implements Watcher
    {
        private final Event.EventType type;
        private final CountDownLatch latch;

        public Trigger(Event.EventType type)
        {
            assertNotNull(type);

            this.type = type;
            this.latch = new CountDownLatch(1);
        }

        @Override
        public void process(WatchedEvent event)
        {
            if ( type == event.getType() )
            {
                latch.countDown();
            }
        }

        public boolean firedWithin(long duration, TimeUnit unit)
        {
            try
            {
                return latch.await(duration, unit);
            }
            catch ( InterruptedException e )
            {
                throw Throwables.propagate(e);
            }
        }

        private static Trigger created()
        {
            return new Trigger(Event.EventType.NodeCreated);
        }

        private static Trigger deleted()
        {
            return new Trigger(Event.EventType.NodeDeleted);
        }

        private static Trigger dataChanged()
        {
            return new Trigger(Event.EventType.NodeDataChanged);
        }
    }
}
