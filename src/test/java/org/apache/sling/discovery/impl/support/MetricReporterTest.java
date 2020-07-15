/*
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
package org.apache.sling.discovery.impl.support;

import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_LOCAL_CLUSTER_INSTANCES;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_LOCAL_CLUSTER_JOINS;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_LOCAL_CLUSTER_LEADER_SWITCHES;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_LOCAL_CLUSTER_LEAVES;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_LOCAL_CLUSTER_PROPERTIES;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_OWN_IS_LEADER;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_OWN_PROPERTIES;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_PROPERTY_CHANGED_EVENTS;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_REMOTE_CLUSTERS;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_REMOTE_INSTANCES;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_TOPOLOGY_CHANGED_EVENTS;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_TOPOLOGY_CHANGING_EVENTS;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_TOPOLOGY_INIT_EVENTS;
import static org.apache.sling.discovery.impl.support.MetricReporter.METRICS_NAME_TOPOLOGY_IS_UNDEFINED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.sling.discovery.ClusterView;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.InstanceFilter;
import org.apache.sling.discovery.commons.providers.BaseTopologyView;
import org.apache.sling.discovery.commons.providers.DefaultClusterView;
import org.apache.sling.discovery.commons.providers.DefaultInstanceDescription;
import org.apache.sling.discovery.commons.providers.ViewStateManager;
import org.apache.sling.discovery.commons.providers.base.ViewStateManagerFactory;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

public class MetricReporterTest {

    static class TestTopologyView extends BaseTopologyView {

        private static final Random propertiesChangerRandom = new Random(54321);

        private final InstanceDescription localInstance;

        private HashSet<ClusterView> clusterViews;

        static Map<String, String> newEmptyProps() {
            final Map<String, String> result = new HashMap<String, String>();
            return result;
        }

        static TestTopologyView singleInstance() {
            return singleInstance(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        }

        static TestTopologyView singleInstance(String localSlingId, String localClusterId) {
            final DefaultClusterView localCluster = new DefaultClusterView(localClusterId);
            final InstanceDescription localInstance = new DefaultInstanceDescription(localCluster, true, true, localSlingId, newEmptyProps());
            return new TestTopologyView(localInstance);
        }

        static BaseTopologyView localClusterWithRandomInstances(int numRandomInstances) {
            return localClusterWithRandomInstances(UUID.randomUUID().toString(), UUID.randomUUID().toString(), numRandomInstances);
        }

        static BaseTopologyView localClusterWithRandomInstances(String localSlingId, String clusterId, int numRandomInstances) {
            final DefaultClusterView localCluster = new DefaultClusterView(clusterId);
            final InstanceDescription localInstance = new DefaultInstanceDescription(localCluster, true, true, localSlingId, newEmptyProps());
            for (int i = 0; i < numRandomInstances; i++) {
                new DefaultInstanceDescription(localCluster, false, false, UUID.randomUUID().toString(), newEmptyProps());
            }

            return new TestTopologyView(localInstance);
        }

        public static BaseTopologyView withLeaderSwitchedBy(BaseTopologyView view, int offset) {
            final ClusterView cluster = view.getLocalInstance().getClusterView();
            int index = 0;
            for (InstanceDescription instance : cluster.getInstances()) {
                if (instance.isLeader()) {
                    break;
                } else {
                    index++;
                }
            }
            int newLeader = (index + offset) % cluster.getInstances().size();
            final DefaultClusterView localCluster = new DefaultClusterView(cluster.getId());
            index = 0;
            InstanceDescription local = null;
            for (InstanceDescription instance : cluster.getInstances()) {
                DefaultInstanceDescription aNewInstance = new DefaultInstanceDescription(localCluster, index++ == newLeader, instance.isLocal(),
                        instance.getSlingId(), instance.getProperties());
                if (instance.isLocal()) {
                    local = aNewInstance;
                }
            }

            return new TestTopologyView(local);
        }

        public static BaseTopologyView withRandomPropertiesAdded(BaseTopologyView view, int offset, int numRandomPropertiesChanged) {
            final ClusterView cluster = view.getLocalInstance().getClusterView();
            final DefaultClusterView localCluster = new DefaultClusterView(cluster.getId());
            int index = 0;
            InstanceDescription local = null;
            for (InstanceDescription instance : cluster.getInstances()) {
                Map<String, String> properties = instance.getProperties();
                if (index++ == offset) {
                    properties = withRandomPropertiesAdded(properties, numRandomPropertiesChanged);
                }
                DefaultInstanceDescription aNewInstance = new DefaultInstanceDescription(localCluster, instance.isLeader(), instance.isLocal(),
                        instance.getSlingId(), properties);
                if (instance.isLocal()) {
                    local = aNewInstance;
                }
            }

            return new TestTopologyView(local);
        }

        private static Map<String, String> withRandomPropertiesAdded(Map<String, String> properties, int numRandomPropertiesChanged) {
            final Map<String, String> result = new HashMap<String, String>(properties);
            for (int i = 0; i < numRandomPropertiesChanged; i++) {
                final String key = "randomKey-" + propertiesChangerRandom.nextInt();
                String current = result.put(key, "r");
                if (current != null) {
                    // then make sure it is different than the current value
                    result.put(key, current + "r");
                }
            }
            return result;
        }

        public static BaseTopologyView multiCluster(int... instanceCounts) {
            final Set<ClusterView> clusters = new HashSet<ClusterView>();
            InstanceDescription localInstance = null;
            for (int i = 0; i < instanceCounts.length; i++) {
                final int instanceCount = instanceCounts[i];

                final DefaultClusterView cluster = new DefaultClusterView(UUID.randomUUID().toString());
                final InstanceDescription firstInstance = new DefaultInstanceDescription(cluster, i == 0, i == 0, UUID.randomUUID().toString(),
                        newEmptyProps());
                if (i == 0) {
                    localInstance = firstInstance;
                }
                for (int j = 0; j < instanceCount - 1; j++) {
                    new DefaultInstanceDescription(cluster, false, false, UUID.randomUUID().toString(), newEmptyProps());
                }

                clusters.add(cluster);
            }

            TestTopologyView view = new TestTopologyView(localInstance);
            view.setClusterViews(clusters);
            return view;
        }

        public static BaseTopologyView addRemote(BaseTopologyView view, int instanceCount) {
            final String localClusterId = view.getLocalInstance().getClusterView().getId();
            final Set<ClusterView> newClusters = new HashSet<ClusterView>();
            final Set<ClusterView> existing = view.getClusterViews();
            InstanceDescription local = null;
            for (ClusterView clusterView : existing) {
                ClusterView clonedCluster = clone(clusterView);
                if (clusterView.getId().equals(localClusterId)) {
                    for (InstanceDescription inst : clonedCluster.getInstances()) {
                        if (inst.getSlingId().equals(view.getLocalInstance().getSlingId())) {
                            local = inst;
                        }
                    }
                }
                newClusters.add(clonedCluster);
            }

            final DefaultClusterView cluster = new DefaultClusterView(UUID.randomUUID().toString());
            for (int j = 0; j < instanceCount; j++) {
                new DefaultInstanceDescription(cluster, false, false, UUID.randomUUID().toString(), newEmptyProps());
            }
            newClusters.add(cluster);

            TestTopologyView newView = new TestTopologyView(local);
            newView.setClusterViews(newClusters);
            return newView;
        }

        private static ClusterView clone(ClusterView original) {
            final DefaultClusterView cluster = new DefaultClusterView(original.getId());
            for (InstanceDescription inst : original.getInstances()) {
                new DefaultInstanceDescription(cluster, inst.isLeader(), inst.isLocal(), inst.getSlingId(), inst.getProperties());
            }
            return cluster;
        }

        private TestTopologyView(InstanceDescription localInstance) {
            this.localInstance = localInstance;
            final Set<ClusterView> result = new HashSet<ClusterView>();
            result.add(localInstance.getClusterView());
            setClusterViews(result);
        }

        @Override
        public InstanceDescription getLocalInstance() {
            return localInstance;
        }

        @Override
        public Set<InstanceDescription> getInstances() {
            // to achieve some true randomness in the way this list is returned,
            // we actually use a Random .. not that this matters much, but
            // would be good to have tests not rely on this
            final Set<InstanceDescription> result = new HashSet<InstanceDescription>();
            final Random r = new Random();
            final List<ClusterView> clusters = new LinkedList<ClusterView>(clusterViews);
            while (!clusters.isEmpty()) {
                final ClusterView cluster = clusters.remove(r.nextInt(clusters.size()));
                for (InstanceDescription instance : cluster.getInstances()) {
                    result.add(instance);
                }
            }
            return result;
        }

        @Override
        public Set<InstanceDescription> findInstances(InstanceFilter filter) {
            throw new IllegalStateException("not implemented");
        }

        @Override
        public Set<ClusterView> getClusterViews() {
            return clusterViews;
        }

        private void setClusterViews(Set<ClusterView> clusterViews) {
            this.clusterViews = new HashSet<ClusterView>(clusterViews);
        }

        @Override
        public String getLocalClusterSyncTokenId() {
            throw new IllegalStateException("not implemented");
        }

    }

    private MetricReporter reporter;
    private MetricRegistry metricRegistry;
    private ViewStateManager viewStateManager;
    private Lock viewStateManagerLock;

    @Before
    public void setup() {
        reporter = new MetricReporter();
        metricRegistry = new MetricRegistry();
        reporter.metricRegistry = metricRegistry;
        viewStateManagerLock = new ReentrantLock();
        viewStateManager = ViewStateManagerFactory.newViewStateManager(viewStateManagerLock, null);
        viewStateManager.bind(reporter);
    }

    @Test
    public void testActivateDeactivate() {
        for (int i = 0; i < 100; i++) {
            reporter.activate();
            reporter.deactivate();
        }
    }

    @Test
    public void testPreInitState() throws InterruptedException {
        reporter.activate();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        viewStateManager.handleActivated();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        reporter.deactivate();
    }

    @Test
    public void testStandardSequence() throws InterruptedException {
        reporter.activate();
        viewStateManager.handleActivated();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        viewStateManager.handleChanging();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        final BaseTopologyView myView = TestTopologyView.singleInstance();
        viewStateManager.handleNewView(myView);
        viewStateManager.waitForAsyncEvents(5000);
        assertGaugesEqual(1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0);
        for (int i = 0; i < 10; i++) {
            viewStateManager.handleChanging();
            viewStateManager.waitForAsyncEvents(5000);
            assertGaugesEqual(1, i, i + 1, 0, 1, 1, 1, 0, 0, 0, 1, 0, 0, 0);
            viewStateManager.handleNewView(
                    TestTopologyView.singleInstance(myView.getLocalInstance().getSlingId(), myView.getLocalInstance().getClusterView().getId()));
            viewStateManager.waitForAsyncEvents(5000);
            assertGaugesEqual(1, i + 1, i + 1, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0);
        }
    }

    @Test
    public void testRandomJoinsLeaves() throws InterruptedException {
        reporter.activate();
        viewStateManager.handleActivated();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        final BaseTopologyView baseView = TestTopologyView.singleInstance();
        viewStateManager.handleNewView(baseView);
        viewStateManager.waitForAsyncEvents(5000);
        assertGaugesEqual(1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 0, 0, 0);

        final Random r = new Random(12345678);
        int joinCnt = 1;
        int leaveCnt = 0;
        for (int i = 0; i < 10; i++) {
            int numRandomInstances = r.nextInt(5) + 1;
            joinCnt += numRandomInstances;
            viewStateManager.handleNewView(TestTopologyView.localClusterWithRandomInstances(baseView.getLocalInstance().getSlingId(),
                    baseView.getLocalInstance().getClusterView().getId(), numRandomInstances));
            viewStateManager.waitForAsyncEvents(5000);
            assertGaugesEqual(1, 2 * i + 1, 2 * i + 1, 0, 0, 1 + numRandomInstances, joinCnt, leaveCnt, 0, 0, 1, 0, 0, 0);

            // back to only my instance - all newly joined leave again
            leaveCnt += numRandomInstances;
            viewStateManager.handleNewView(
                    TestTopologyView.singleInstance(baseView.getLocalInstance().getSlingId(), baseView.getLocalInstance().getClusterView().getId()));
            viewStateManager.waitForAsyncEvents(5000);
            assertGaugesEqual(1, 2 * i + 2, 2 * i + 2, 0, 0, 1, joinCnt, leaveCnt, 0, 0, 1, 0, 0, 0);
        }
    }

    @Test
    public void testLeader() throws InterruptedException {
        reporter.activate();
        viewStateManager.handleActivated();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        final int num = 11;
        BaseTopologyView view = TestTopologyView.localClusterWithRandomInstances(num - 1);
        viewStateManager.handleNewView(view);
        viewStateManager.waitForAsyncEvents(5000);
        assertGaugesEqual(1, 0, 0, 0, 0, num, num, 0, 0, 0, 1, 0, 0, 0);

        for (int i = 0; i < 10; i++) {
            int offset = 1;
            view = TestTopologyView.withLeaderSwitchedBy(view, offset);
            viewStateManager.handleNewView(view);
            viewStateManager.waitForAsyncEvents(5000);
            assertGaugesEqual(1, i + 1, i + 1, 0, 0, num, num, 0, i + 1, 0, view.getLocalInstance().isLeader() ? 1 : 0, 0, 0, 0);
        }
    }

    @Test
    public void testInitialAndChangedProperties() throws InterruptedException {
        reporter.activate();
        viewStateManager.handleActivated();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        BaseTopologyView view = TestTopologyView.localClusterWithRandomInstances(3);
        view = TestTopologyView.withRandomPropertiesAdded(view, 0, 4);
        view = TestTopologyView.withRandomPropertiesAdded(view, 1, 3);
        view = TestTopologyView.withRandomPropertiesAdded(view, 2, 8);
        viewStateManager.handleNewView(view);
        viewStateManager.waitForAsyncEvents(5000);
        assertGaugesEqual(1, 0, 0, 0, 0, 3 + 1, 3 + 1, 0, 0, 4 + 3 + 8, 1, 4, 0, 0);

        view = TestTopologyView.withRandomPropertiesAdded(view, 1, 7);
        view = TestTopologyView.addRemote(view, 1);
        viewStateManager.handleNewView(view);
        viewStateManager.waitForAsyncEvents(5000);
        assertGaugesEqual(1, 1, 1, 0, 0, 3 + 1, 3 + 1, 0, 0, 4 + 3 + 8 + 7, 1, 4, 1, 1);
    }

    @Test
    public void testProperties() throws InterruptedException {
        reporter.activate();
        viewStateManager.handleActivated();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        final int num = 11;
        BaseTopologyView view = TestTopologyView.localClusterWithRandomInstances(num - 1);
        viewStateManager.handleNewView(view);
        viewStateManager.waitForAsyncEvents(5000);
        assertGaugesEqual(1, 0, 0, 0, 0, num, num, 0, 0, 0, 1, 0, 0, 0);

        final Random r = new Random(100000);
        for (int i = 0; i < num; i++) {
            int numRandomPropertiesChanged = r.nextInt(10);
            view = TestTopologyView.withRandomPropertiesAdded(view, i, numRandomPropertiesChanged);
            viewStateManager.handleNewView(view);
            viewStateManager.waitForAsyncEvents(5000);
            int localPropertiesCount = localProperties(view);
            assertTrue("no properties set", localPropertiesCount > 0);
            assertGaugesEqual(1, 0, 0, i + 1, 0, num, num, 0, 0, localPropertiesCount, view.getLocalInstance().isLeader() ? 1 : 0,
                    view.getLocalInstance().getProperties().size(), 0, 0);
        }
    }

    @Test
    public void testRemoteCluster() throws InterruptedException {
        reporter.activate();
        viewStateManager.handleActivated();
        assertGaugesEqual(0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        BaseTopologyView view = TestTopologyView.multiCluster(3, 2, 4);
        viewStateManager.handleNewView(view);
        viewStateManager.waitForAsyncEvents(5000);
        assertGaugesEqual(1, 0, 0, 0, 0, view.getLocalInstance().getClusterView().getInstances().size(),
                view.getLocalInstance().getClusterView().getInstances().size(), 0, 0, 0, 1, 0, 2, 6);

        view = TestTopologyView.addRemote(view, 5);
        viewStateManager.handleNewView(view);
        viewStateManager.waitForAsyncEvents(5000);
        assertGaugesEqual(1, 1, 1, 0, 0, view.getLocalInstance().getClusterView().getInstances().size(),
                view.getLocalInstance().getClusterView().getInstances().size(), 0, 0, 0, 1, 0, 3, 11);
    }

    private int localProperties(BaseTopologyView view) {
        int properties = 0;
        for (InstanceDescription instance : view.getLocalInstance().getClusterView().getInstances()) {
            properties += instance.getProperties().size();
        }
        return properties;
    }

    private void assertGaugesEqual(int init, int changed, int changing, long propertyChanged, int isUndefined, int localInstances, int localJoins,
            int localLeaves, int localLeaderSwitches, int localPropertiesCount, int ownIsLeader, int ownPropertiesCount, int remoteClustersCount,
            int remoteInstancesCount) throws InterruptedException {
        long maxWait = System.currentTimeMillis() + 5000;
        while (maxWait < System.currentTimeMillis()) {
            try {
                assertGaugesEqualNoWait(init, changed, changing, propertyChanged, isUndefined, localInstances, localJoins, localLeaves,
                        localLeaderSwitches, localPropertiesCount, ownIsLeader, ownPropertiesCount, remoteClustersCount, remoteInstancesCount);
                // successful case
                return;
            } catch (Throwable th) {
                // error case : sleep and retry
                Thread.sleep(100);
            }
        }
        // timeout case : one last retry, outside try/catch to propagate the failure
        assertGaugesEqualNoWait(init, changed, changing, propertyChanged, isUndefined, localInstances, localJoins, localLeaves, localLeaderSwitches,
                localPropertiesCount, ownIsLeader, ownPropertiesCount, remoteClustersCount, remoteInstancesCount);
    }

    private void assertGaugesEqualNoWait(int init, int changed, int changing, long propertyChanged, int isUndefined, int localInstances,
            int localJoins, int localLeaves, int localLeaderSwitches, int localPropertiesCount, int ownIsLeader, int ownPropertiesCount,
            int remoteClustersCount, int remoteInstancesCount) {
        assertGaugeEquals(init, METRICS_NAME_TOPOLOGY_INIT_EVENTS);
        assertGaugeEquals(changed, METRICS_NAME_TOPOLOGY_CHANGED_EVENTS);
        assertGaugeEquals(changing, METRICS_NAME_TOPOLOGY_CHANGING_EVENTS);
        assertGaugeEquals(propertyChanged, METRICS_NAME_PROPERTY_CHANGED_EVENTS);

        assertGaugeEquals(isUndefined, METRICS_NAME_TOPOLOGY_IS_UNDEFINED);

        assertGaugeEquals(localInstances, METRICS_NAME_LOCAL_CLUSTER_INSTANCES);
        assertGaugeEquals(localJoins, METRICS_NAME_LOCAL_CLUSTER_JOINS);
        assertGaugeEquals(localLeaves, METRICS_NAME_LOCAL_CLUSTER_LEAVES);
        assertGaugeEquals(localLeaderSwitches, METRICS_NAME_LOCAL_CLUSTER_LEADER_SWITCHES);
        assertGaugeEquals(localPropertiesCount, METRICS_NAME_LOCAL_CLUSTER_PROPERTIES);

        assertGaugeEquals(ownIsLeader, METRICS_NAME_OWN_IS_LEADER);
        assertGaugeEquals(ownPropertiesCount, METRICS_NAME_OWN_PROPERTIES);

        assertGaugeEquals(remoteClustersCount, METRICS_NAME_REMOTE_CLUSTERS);
        assertGaugeEquals(remoteInstancesCount, METRICS_NAME_REMOTE_INSTANCES);
    }

    private void assertGaugeEquals(int expected, String metricsName) {
        assertEquals(metricsName, expected, metricRegistry.getGauges().get(metricsName).getValue());
    }

    private void assertGaugeEquals(long expected, String metricsName) {
        assertEquals(metricsName, expected, metricRegistry.getGauges().get(metricsName).getValue());
    }
}
