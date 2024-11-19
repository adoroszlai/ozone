/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.scm.safemode;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.server.SCMDatanodeProtocolServer.NodeRegistrationContainerReport;
import org.apache.hadoop.hdds.server.events.EventQueue;
import org.apache.hadoop.hdds.server.events.TypedEvent;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class defining Safe mode exit criteria for Containers.
 */
public abstract class ContainerSafeModeRule extends
    SafeModeExitRule<NodeRegistrationContainerReport> {

  public static final Logger LOG =
      LoggerFactory.getLogger(ContainerSafeModeRule.class);
  // Required cutoff % for containers with at least 1 reported replica.
  private final double safeModeCutoff;
  // Containers read from scm db (excluding containers in ALLOCATED state).
  private final Map<Long, Integer> containerMinReplicaMap;
  private final Map<Long, Set<UUID>> containerDNsMap;
  private double maxContainer;
  private final AtomicLong containerWithMinReplicas = new AtomicLong(0);
  private final ReplicationType type;
  private final ContainerManager containerManager;

  protected abstract void updateCutoffMetrics(long cutOff);
  protected abstract void updateReportedContainerMetrics();

  protected ContainerSafeModeRule(String ruleName, EventQueue eventQueue,
             ReplicationType type,
             ConfigurationSource conf,
             List<ContainerInfo> containers,
             ContainerManager containerManager, SCMSafeModeManager manager) {
    super(manager, ruleName, eventQueue);

    this.type = type;

    this.containerManager = containerManager;
    safeModeCutoff = conf.getDouble(
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT,
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT_DEFAULT);

    Preconditions.checkArgument(
        (safeModeCutoff >= 0.0 && safeModeCutoff <= 1.0),
        HddsConfigKeys.HDDS_SCM_SAFEMODE_THRESHOLD_PCT  +
            " value should be >= 0.0 and <= 1.0");

    containerMinReplicaMap = new ConcurrentHashMap<>();
    containerDNsMap = new ConcurrentHashMap<>();

    initializeRule(containers);
  }

  @Override
  protected TypedEvent<NodeRegistrationContainerReport> getEventType() {
    return SCMEvents.CONTAINER_REGISTRATION_REPORT;
  }

  @Override
  protected synchronized boolean validate() {
    return getCurrentContainerThreshold() >= safeModeCutoff;
  }

  @VisibleForTesting
  public synchronized double getCurrentContainerThreshold() {
    if (maxContainer == 0) {
      return 1;
    }
    return (containerWithMinReplicas.doubleValue() / maxContainer);
  }

  private synchronized double getMaxContainer() {
    if (maxContainer == 0) {
      return 1;
    }
    return maxContainer;
  }

  @Override
  protected synchronized void process(
      NodeRegistrationContainerReport reportsProto) {
    DatanodeDetails datanodeDetails = reportsProto.getDatanodeDetails();
    UUID datanodeUUID = datanodeDetails.getUuid();
    StorageContainerDatanodeProtocolProtos.ContainerReportsProto report = reportsProto.getReport();

    report.getReportsList().forEach(c -> {
      Long containerID = c.getContainerID();

      if (containerMinReplicaMap.containsKey(containerID)) {
        putInContainerDNsMap(containerID, datanodeUUID);
        recordReportedContainer(containerID);
      }
    });

    if (scmInSafeMode()) {
      SCMSafeModeManager.getLogger().info(
          "SCM in safe mode. {} % of {} containers have at least the minimum number of replicas reported.",
          ((containerWithMinReplicas.doubleValue() / getMaxContainer()) * 100), type
      );
    }
  }

  /**
   * Record the reported Container.
   *
   * We will differentiate and count according to the type of Container.
   *
   * @param containerID containerID
   */
  private void recordReportedContainer(long containerID) {
    Set<UUID> uuids = containerDNsMap.get(containerID);
    int minReplica = getMinReplica(containerID);
    if (uuids != null && uuids.size() >= minReplica) {
      containerWithMinReplicas.getAndAdd(1);
      updateReportedContainerMetrics();
    }
  }

  /**
   * Get the minimum replica.
   *
   * If it is a Ratis Contianer, the minimum copy is 1.
   * If it is an EC Container, the minimum copy will be the number of Data in replicationConfig.
   *
   * @param containerID containerID
   * @return MinReplica.
   */
  private int getMinReplica(Long containerID) {
    return containerMinReplicaMap.getOrDefault(containerID, 1);
  }

  private void putInContainerDNsMap(Long containerID, UUID datanodeUUID) {
    containerDNsMap.computeIfAbsent(containerID, key -> Sets.newHashSet())
        .add(datanodeUUID);
  }

  @Override
  protected synchronized void cleanup() {
    containerMinReplicaMap.clear();
    containerDNsMap.clear();
  }

  @Override
  public String getStatusText() {

    // ratis container
    String status = String.format(
        "%1.2f%% of [%s] Containers(%s / %s) with at least the minimum number of replicas reported (=%1.2f) >= " +
        "safeModeCutoff (=%1.2f);",
        (containerWithMinReplicas.doubleValue() / getMaxContainer()) * 100,
        type,
        containerWithMinReplicas, (long) getMaxContainer(),
        getCurrentContainerThreshold(), this.safeModeCutoff);

    Set<Long> sampleContainers = containerDNsMap.entrySet().stream()
        .filter(entry -> entry.getValue().size() < getMinReplica(entry.getKey()))
        .map(Map.Entry::getKey)
        .limit(SAMPLE_CONTAINER_DISPLAY_LIMIT)
        .collect(Collectors.toSet());

    if (!sampleContainers.isEmpty()) {
      String sampleContainerText =
          "Sample Containers not satisfying the criteria : " + sampleContainers + ";";
      status = status.concat("\n").concat(sampleContainerText);
    }

    return status;
  }


  @Override
  public synchronized void refresh(boolean forceRefresh) {
    List<ContainerInfo> containers = containerManager.getContainers();
    if (forceRefresh) {
      initializeRule(containers);
    } else {
      if (!validate()) {
        initializeRule(containers);
      }
    }
  }

  private boolean checkContainerState(LifeCycleState state) {
    return state == LifeCycleState.QUASI_CLOSED || state == LifeCycleState.CLOSED;
  }

  private void initializeRule(List<ContainerInfo> containers) {

    // Clean up the related data in the map.
    containerMinReplicaMap.clear();

    HddsProtos.ReplicationType protoType = ReplicationType.toProto(type);

    // Iterate through the container list to
    // get the minimum replica count for each container.
    containers.forEach(container -> {
      // There can be containers in OPEN/CLOSING state which were never
      // created by the client. We are not considering these containers for
      // now. These containers can be handled by tracking pipelines.

      LifeCycleState containerState = container.getState();
      ReplicationConfig replicationConfig = container.getReplicationConfig();
      HddsProtos.ReplicationType replicationType = container.getReplicationType();

      if (protoType.equals(replicationType)
          && checkContainerState(containerState)
          && container.getNumberOfKeys() > 0) {
        containerMinReplicaMap.put(container.getContainerID(), replicationConfig.getMinimumNodes());
      }
    });

    maxContainer = containerMinReplicaMap.size();

    long cutOff = (long) Math.ceil(maxContainer * safeModeCutoff);

    updateCutoffMetrics(cutOff);

    LOG.info("Refreshed {} Containers with threshold count {}.", type, cutOff);
  }

  /** Safemode rule for Ratis containers. */
  public static class Ratis extends ContainerSafeModeRule {

    public Ratis(
        String ruleName,
        EventQueue eventQueue,
        ConfigurationSource conf,
        List<ContainerInfo> containers,
        ContainerManager containerManager,
        SCMSafeModeManager manager
    ) {
      super(ruleName, eventQueue, ReplicationType.RATIS, conf, containers, containerManager, manager);
    }

    @Override
    protected void updateCutoffMetrics(long cutOff) {
      getSafeModeMetrics().setNumContainerWithOneReplicaReportedThreshold(cutOff);
    }

    @Override
    protected void updateReportedContainerMetrics() {
      getSafeModeMetrics().incCurrentContainersWithOneReplicaReportedCount();
    }
  }

  /** Safemode rule for EC containers. */
  public static class EC extends ContainerSafeModeRule {

    public EC(
        String ruleName,
        EventQueue eventQueue,
        ConfigurationSource conf,
        List<ContainerInfo> containers,
        ContainerManager containerManager,
        SCMSafeModeManager manager
    ) {
      super(ruleName, eventQueue, ReplicationType.EC, conf, containers, containerManager, manager);
    }

    @Override
    protected void updateCutoffMetrics(long cutOff) {
      getSafeModeMetrics().setNumContainerWithECDataReplicaReportedThreshold(cutOff);
    }

    @Override
    protected void updateReportedContainerMetrics() {
      getSafeModeMetrics().incCurrentContainersWithECDataReplicaReportedCount();
    }
  }
}
