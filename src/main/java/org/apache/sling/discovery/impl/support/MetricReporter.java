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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.sling.discovery.ClusterView;
import org.apache.sling.discovery.InstanceDescription;
import org.apache.sling.discovery.TopologyEvent;
import org.apache.sling.discovery.TopologyEventListener;
import org.apache.sling.discovery.TopologyView;
import org.apache.sling.discovery.commons.InstancesDiff;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/**
 * MetricReporter is in charge of listening to TopologyEvents
 * (by virtue of being a TopologyEventListener) and exposing
 * metrics (via dropwizard).
 */
@Component(immediate = true, service = { TopologyEventListener.class })
public class MetricReporter implements TopologyEventListener {

    // event counters
    static final String METRICS_NAME_TOPOLOGY_CHANGING_EVENTS = "discovery.oak.topology.changing.events";
    static final String METRICS_NAME_TOPOLOGY_INIT_EVENTS = "discovery.oak.topology.init.events";
    static final String METRICS_NAME_TOPOLOGY_CHANGED_EVENTS = "discovery.oak.topology.changed.events";
    static final String METRICS_NAME_PROPERTY_CHANGED_EVENTS = "discovery.oak.property.changed.events";

    static final String METRICS_NAME_TOPOLOGY_IS_UNDEFINED = "discovery.oak.topology.is.undefined";

    static final String METRICS_NAME_LOCAL_CLUSTER_INSTANCES = "discovery.oak.local.cluster.instances";
    static final String METRICS_NAME_LOCAL_CLUSTER_JOINS = "discovery.oak.local.cluster.joins";
    static final String METRICS_NAME_LOCAL_CLUSTER_LEAVES = "discovery.oak.local.cluster.leaves";
    static final String METRICS_NAME_LOCAL_CLUSTER_LEADER_SWITCHES = "discovery.oak.local.cluster.leader.switches";
    static final String METRICS_NAME_LOCAL_CLUSTER_PROPERTIES = "discovery.oak.local.cluster.properties";

    static final String METRICS_NAME_OWN_IS_LEADER = "discovery.oak.own.is.leader";
    static final String METRICS_NAME_OWN_PROPERTIES = "discovery.oak.own.properties";

    static final String METRICS_NAME_REMOTE_CLUSTERS = "discovery.oak.remote.cluster";
    static final String METRICS_NAME_REMOTE_INSTANCES = "discovery.oak.remote.instances";

    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Reference(target = "(name=sling)")
    MetricRegistry metricRegistry;

    private final List<String> registeredGaugeNameList = new LinkedList<>();

    /** 
     * for init there would really only be 2 values needed: 0 and 1
     * as there should only ever be 1 TOPOLOGY_INIT event.
     * But for monitoring reasons it might be interesting to use a 
     * counter here nevertheless and ensure that it never goes above 1.
     */
    private final AtomicInteger initEvents = new AtomicInteger(0);

    /** 
     * counts number of TOPOLOGY_CHANGING events. An int should be enough,
     * if there is 1 event per second this lasts 68 years
     */
    private final AtomicInteger changingEvents = new AtomicInteger(0);

    /** 
     * counts number of TOPOLOGY_CHANGED events. An int should be enough,
     * if there is 1 event per second this lasts 68 years
     */
    private final AtomicInteger changedEvents = new AtomicInteger(0);

    /** 
     * counts number of TOPOLOGY_CHANGED events. 
     * With a long if there is 1 event per millisecond it lasts 292471208 years
     */
    private final AtomicLong propertyChangedEvents = new AtomicLong(0);

    /**
     * This is either 0 or 1 - but since the Gauge should be as fast as possible
     * we maintain an int.
     * Note that if the topology is undefined the localLeader is also undefined
     * (but still represents the previous value).
     * This one starts off as 1 until TOPOLOGY_INIT is received.
     */
    private final AtomicInteger topologyIsUndefined = new AtomicInteger(1);

    /** 
     * Keeps track of number of instances in local cluster.
     * There should really only be a small number, but even so an int is certainly enough.
     */
    private final AtomicInteger localClusterInstances = new AtomicInteger(0);

    /**
     * Counts the number of instances that joined the local cluster, over time.
     * The order of magnitude is number of TOPOLOGY_CHANGED events multiplied
     * by number of instances joining per such event. Still, an int sounds more than enough.
     */
    private final AtomicInteger localClusterJoins = new AtomicInteger(0);

    /**
     * Counts the number of instances that left the local cluster, over time.
     * The order of magnitude is number of TOPOLOGY_CHANGED events multiplied
     * by number of instances leaving per such event. Still, an int sounds more than enough.
     */
    private final AtomicInteger localClusterLeaves = new AtomicInteger(0);

    /**
     * The number of leader changes should be smaller or equal to
     * TOPOLOGY_CHANGED events +1.
     * So an int seems more than enough.
     * Note that this counts only actual changes, not a leader being announced via TOPOLOGY_INIT.
     */
    private final AtomicInteger localClusterLeaderSwitches = new AtomicInteger(0);

    /**
     * The order of magnitude here is number of properties multiplied by number of instances in the local cluster.
     * So this is an order of magnitude higher than ownPropertiesCount -
     * but still, an int should be enough, really.
     */
    private final AtomicInteger localClusterProperties = new AtomicInteger(0);

    /**
     * This is either 0 or 1 - but since the Gauge should be as fast as possible
     * we maintain an int.
     * Note that localLeader is only valid if the topology is not changing currently
     * (otherwise it is undefined).
     */
    private final AtomicInteger ownIsLeader = new AtomicInteger(0);

    /**
     * There shouldn't be an awful lot of properties, so int sounds more than enough
     */
    private final AtomicInteger ownProperties = new AtomicInteger(0);

    /**
     * Attached/remote clusters aren't probably too many, so again, int is enough
     */
    private final AtomicInteger remoteClusters = new AtomicInteger(0);

    /**
     * Attached/remote instances (sum of instances in remote clusters) -
     * probably aren't too many, int is enough
     */
    private final AtomicInteger remoteInstances = new AtomicInteger(0);

    @Activate
    protected void activate() {
        logger.debug("activate: start");

        createGauge(METRICS_NAME_TOPOLOGY_INIT_EVENTS, (Gauge<Integer>) () -> initEvents.get());

        createGauge(METRICS_NAME_TOPOLOGY_CHANGING_EVENTS, (Gauge<Integer>) () -> changingEvents.get());

        createGauge(METRICS_NAME_TOPOLOGY_CHANGED_EVENTS, (Gauge<Integer>) () -> changedEvents.get());

        createGauge(METRICS_NAME_PROPERTY_CHANGED_EVENTS, (Gauge<Long>) () -> propertyChangedEvents.get());

        createGauge(METRICS_NAME_TOPOLOGY_IS_UNDEFINED, (Gauge<Integer>) () -> topologyIsUndefined.get());

        createGauge(METRICS_NAME_LOCAL_CLUSTER_INSTANCES, (Gauge<Integer>) () -> localClusterInstances.get());

        createGauge(METRICS_NAME_LOCAL_CLUSTER_JOINS, (Gauge<Integer>) () -> localClusterJoins.get());

        createGauge(METRICS_NAME_LOCAL_CLUSTER_LEAVES, (Gauge<Integer>) () -> localClusterLeaves.get());

        createGauge(METRICS_NAME_LOCAL_CLUSTER_LEADER_SWITCHES, (Gauge<Integer>) () -> localClusterLeaderSwitches.get());

        createGauge(METRICS_NAME_LOCAL_CLUSTER_PROPERTIES, (Gauge<Integer>) () -> localClusterProperties.get());

        createGauge(METRICS_NAME_OWN_IS_LEADER, (Gauge<Integer>) () -> ownIsLeader.get());

        createGauge(METRICS_NAME_OWN_PROPERTIES, (Gauge<Integer>) () -> ownProperties.get());

        createGauge(METRICS_NAME_REMOTE_CLUSTERS, (Gauge<Integer>) () -> remoteClusters.get());

        createGauge(METRICS_NAME_REMOTE_INSTANCES, (Gauge<Integer>) () -> remoteInstances.get());

        logger.info("activate: done.");
    }

    @SuppressWarnings("rawtypes")
    private void createGauge(String gaugeName, Gauge gauge) {
        logger.debug("createGauge: registering gauge : " + gaugeName);
        this.metricRegistry.register(gaugeName, gauge);
        registeredGaugeNameList.add(gaugeName);
    }

    @Deactivate
    protected void deactivate() {
        logger.debug("deactivate: deactivating.");
        unregisterGauges();
        logger.info("deactivate: done.");
    }

    private void unregisterGauges() {
        for (String registeredGaugeName : registeredGaugeNameList) {
            logger.debug("unregisterGauges : unregistering gauge : " + registeredGaugeName);
            metricRegistry.remove(registeredGaugeName);
        }
    }

    @Override
    public void handleTopologyEvent(TopologyEvent event) {
        if (event == null) {
            // this should not occur
            return;
        }

        try {
            switch (event.getType()) {
            case TOPOLOGY_INIT: {
                handleInit(event.getNewView());
                return;
            }
            case TOPOLOGY_CHANGING: {
                handleChanging();
                return;
            }
            case TOPOLOGY_CHANGED: {
                handleChanged(event.getOldView(), event.getNewView());
                return;
            }
            case PROPERTIES_CHANGED: {
                handlePropertiesChanged(event.getNewView());
                return;
            }
            }
        } catch (Exception e) {
            // we should not really see any of those, but just in case..:
            logger.error("handleTopologyEvent: got Exception " + e, e);
        }
    }

    private void handleInit(TopologyView newView) {
        initEvents.incrementAndGet();
        topologyIsUndefined.set(0);

        updateLocalClusterInstances(null, newView);

        updateProperties(newView);
        updateOwnIsLeader(newView);
        updateRemote(newView);
    }

    private void handleChanging() {
        changingEvents.incrementAndGet();
        topologyIsUndefined.set(1);
    }

    private void handleChanged(TopologyView oldView, TopologyView newView) {
        changedEvents.incrementAndGet();
        topologyIsUndefined.set(0);

        updateLocalClusterInstances(oldView, newView);

        updateLeaderSwitch(oldView, newView);

        updateProperties(newView);
        updateRemote(newView);
    }

    private void handlePropertiesChanged(TopologyView newView) {
        propertyChangedEvents.incrementAndGet();

        updateProperties(newView);
    }

    private void updateLocalClusterInstances(TopologyView oldViewOrNull, TopologyView newView) {
        final ClusterView newLocalClusterView = newView.getLocalInstance().getClusterView();
        localClusterInstances.set(newLocalClusterView.getInstances().size());

        if (oldViewOrNull == null) {
            localClusterJoins.addAndGet(newLocalClusterView.getInstances().size());
        } else {
            final ClusterView oldLocalClusterView = oldViewOrNull.getLocalInstance().getClusterView();
            final InstancesDiff diff = new InstancesDiff(oldLocalClusterView, newLocalClusterView);
            final Collection<InstanceDescription> added = diff.added().get();
            final Collection<InstanceDescription> removed = diff.removed().get();

            if (added.size() > 0) {
                localClusterJoins.addAndGet(added.size());
            }
            if (removed.size() > 0) {
                localClusterLeaves.addAndGet(removed.size());
            }
        }
    }

    private void updateLeaderSwitch(TopologyView oldView, TopologyView newView) {
        final InstanceDescription oldLeader = oldView.getLocalInstance().getClusterView().getLeader();
        final InstanceDescription newLeader = newView.getLocalInstance().getClusterView().getLeader();
        if (!oldLeader.getSlingId().equals(newLeader.getSlingId())) {
            localClusterLeaderSwitches.incrementAndGet();
        }

        updateOwnIsLeader(newView);
    }

    private void updateOwnIsLeader(TopologyView newView) {
        if (newView.getLocalInstance().isLeader()) {
            ownIsLeader.set(1);
        } else {
            ownIsLeader.set(0);
        }
    }

    private void updateProperties(TopologyView newView) {
        ownProperties.set(newView.getLocalInstance().getProperties().size());
        final ClusterView localCluster = newView.getLocalInstance().getClusterView();
        int properties = 0;
        for (InstanceDescription instance : localCluster.getInstances()) {
            properties += instance.getProperties().size();
        }
        localClusterProperties.set(properties);
    }

    private void updateRemote(TopologyView newView) {
        // remoteClusters only counts the remote ones, so we subtract 1 representing our local one
        remoteClusters.set(newView.getClusterViews().size() - 1);
        final String localClusterId = newView.getLocalInstance().getClusterView().getId();
        int instances = 0;
        for (ClusterView cluster : newView.getClusterViews()) {
            final String clusterId = cluster.getId();
            if (!clusterId.equals(localClusterId)) {
                instances += cluster.getInstances().size();
            }
        }
        remoteInstances.set(instances);
    }

}
