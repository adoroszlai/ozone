/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.Config;
import org.apache.hadoop.hdds.conf.ConfigGroup;
import org.apache.hadoop.hdds.conf.ConfigTag;
import org.apache.hadoop.hdds.conf.ConfigType;
import org.apache.hadoop.hdds.conf.PostConstruct;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.PipelineRequestInformation;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.node.NodeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.conf.ConfigTag.SCM;

/**
 * Writable Container provider to obtain a writable container for EC pipelines.
 */
public class WritableECContainerProvider
    implements WritableContainerProvider<ECReplicationConfig> {

  private static final Logger LOG = LoggerFactory
      .getLogger(WritableECContainerProvider.class);

  private final NodeManager nodeManager;
  private final PipelineManager pipelineManager;
  private final PipelineChoosePolicy pipelineChoosePolicy;
  private final ContainerManager containerManager;
  private final long containerSize;
  private final WritableECContainerProviderConfig providerConfig;
  private final AtomicInteger pendingAllocations = new AtomicInteger();

  public WritableECContainerProvider(WritableECContainerProviderConfig config,
      long containerSize,
      NodeManager nodeManager,
      PipelineManager pipelineManager,
      ContainerManager containerManager,
      PipelineChoosePolicy pipelineChoosePolicy) {
    this.providerConfig = config;
    this.nodeManager = nodeManager;
    this.pipelineManager = pipelineManager;
    this.containerManager = containerManager;
    this.pipelineChoosePolicy = pipelineChoosePolicy;
    this.containerSize = containerSize;
  }

  /**
   * Find an existing container suitable for the request.  Possibly allocate
   * a new one, too.  Return one of them randomly.
   */
  @Override
  public ContainerInfo getContainer(final long size,
      ECReplicationConfig repConfig, String owner, ExcludeList excludeList)
      throws IOException, TimeoutException {

    ContainerInfo existing = selectExisting(size, repConfig, excludeList);
    ContainerInfo newContainer = mayAllocate(getMaximumPipelines(repConfig),
        repConfig, size, owner, excludeList);
    return choose(existing, newContainer);
  }

  @Nullable
  private ContainerInfo choose(
      @Nullable ContainerInfo existing,
      @Nullable ContainerInfo newOne
  ) {
    if (existing == null) {
      return newOne;
    }
    if (newOne == null) {
      return existing;
    }
    // TODO tweak %
    return ThreadLocalRandom.current().nextInt(0, 100) < 33
        ? newOne : existing;
  }

  @Nullable
  private ContainerInfo selectExisting(final long size,
      ECReplicationConfig repConfig, ExcludeList excludeList)
      throws IOException, TimeoutException {

    List<Pipeline> existingPipelines = pipelineManager.getPipelines(
        repConfig, Pipeline.PipelineState.OPEN,
        excludeList.getDatanodes(), excludeList.getPipelineIds());

    PipelineRequestInformation pri =
        PipelineRequestInformation.Builder.getBuilder()
            .setSize(size)
            .build();
    while (existingPipelines.size() > 0) {
      int pipelineIndex =
          pipelineChoosePolicy.choosePipelineIndex(existingPipelines, pri);
      if (pipelineIndex < 0) {
        LOG.warn("Unable to select a pipeline from {} in the list",
            existingPipelines.size());
        break;
      }
      Pipeline pipeline = existingPipelines.get(pipelineIndex);
      synchronized (pipeline.getId()) {
        try {
          ContainerInfo containerInfo = getContainerFromPipeline(pipeline);
          if (containerInfo == null
              || !containerHasSpace(containerInfo, size)) {
            existingPipelines.remove(pipelineIndex);
            pipelineManager.closePipeline(pipeline, true);
          } else {
            if (containerIsExcluded(containerInfo, excludeList)) {
              existingPipelines.remove(pipelineIndex);
            } else {
              containerInfo.updateLastUsedTime();
              return containerInfo;
            }
          }
        } catch (PipelineNotFoundException | ContainerNotFoundException e) {
          LOG.warn("Pipeline or container not found when selecting a writable "
              + "container", e);
          existingPipelines.remove(pipelineIndex);
          pipelineManager.closePipeline(pipeline, true);
        }
      }
    }
    return null;
  }

  @Nullable
  private ContainerInfo mayAllocate(int max,
      ECReplicationConfig repConfig, long size,
      String owner, ExcludeList excludeList) {

    int current = pipelineManager.getPipelineCount(repConfig,
        Pipeline.PipelineState.OPEN);

    final int pending = pendingAllocations.getAndIncrement();
    try {
      if (current + pending < max) {
        return allocateContainer(repConfig, size, owner, excludeList);
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Unable to allocate a container for {} as {} existing "
              + "containers and {} pending allocations have reached the limit "
              + "of {}", repConfig, current, pending, max);
        }
        return null;
      }
    } catch (Exception e) {
      LOG.warn("Error trying to allocate a container for {}", repConfig, e);
      return null;
    } finally {
      pendingAllocations.decrementAndGet();
    }
  }

  private int getMaximumPipelines(ECReplicationConfig repConfig) {
    final double factor = providerConfig.getPipelinePerVolumeFactor();
    int volumeBasedCount = 0;
    if (factor > 0) {
      int volumes = nodeManager.totalHealthyVolumeCount();
      volumeBasedCount = (int) factor * volumes / repConfig.getRequiredNodes();
    }
    return Math.max(volumeBasedCount, providerConfig.getMinimumPipelines());
  }

  private ContainerInfo allocateContainer(ReplicationConfig repConfig,
      long size, String owner, ExcludeList excludeList)
      throws IOException, TimeoutException {

    List<DatanodeDetails> excludedNodes = Collections.emptyList();
    if (excludeList.getDatanodes().size() > 0) {
      excludedNodes = new ArrayList<>(excludeList.getDatanodes());
    }

    Pipeline newPipeline = pipelineManager.createPipeline(repConfig,
        excludedNodes, Collections.emptyList());
    ContainerInfo container =
        containerManager.getMatchingContainer(size, owner, newPipeline);
    pipelineManager.openPipeline(newPipeline.getId());
    return container;
  }

  private boolean containerIsExcluded(ContainerInfo container,
      ExcludeList excludeList) {
    return excludeList.getContainerIds().contains(container.containerID());
  }

  @Nullable
  private ContainerInfo getContainerFromPipeline(Pipeline pipeline)
      throws IOException {
    // Assume the container is still open if the below method returns it. On
    // container FINALIZE, ContainerManager will remove the container from the
    // pipeline list in PipelineManager. Finalize can be triggered by a DN
    // sending a message that the container is full, or on close pipeline or
    // on a stale / dead node event (via close pipeline).
    NavigableSet<ContainerID> containers =
        pipelineManager.getContainersInPipeline(pipeline.getId());
    // Assume 1 container per pipeline for EC
    if (containers.size() == 0) {
      return null;
    }
    ContainerID containerID = containers.first();
    return containerManager.getContainer(containerID);
  }

  private boolean containerHasSpace(ContainerInfo container, long size) {
    // The size passed from OM will be the cluster block size. Therefore we
    // just check if the container has enough free space to accommodate another
    // full block.
    return container.getUsedBytes() + size <= containerSize;
  }

  /**
   * Class to hold configuration for WriteableECContainerProvider.
   */
  @ConfigGroup(prefix = WritableECContainerProviderConfig.PREFIX)
  public static class WritableECContainerProviderConfig {

    private static final String PREFIX = "ozone.scm.ec";

    @Config(key = "pipeline.minimum",
        defaultValue = "5",
        type = ConfigType.INT,
        description = "The minimum number of pipelines to have open for each " +
            "Erasure Coding configuration",
        tags = ConfigTag.STORAGE)
    private int minimumPipelines = 5;

    public int getMinimumPipelines() {
      return minimumPipelines;
    }

    public void setMinimumPipelines(int minPipelines) {
      this.minimumPipelines = minPipelines;
    }

    private static final String PIPELINE_PER_VOLUME_FACTOR_KEY =
        "pipeline.per.volume.factor";
    private static final double PIPELINE_PER_VOLUME_FACTOR_DEFAULT = 1;
    private static final String PIPELINE_PER_VOLUME_FACTOR_DEFAULT_VALUE = "1";
    private static final String EC_PIPELINE_PER_VOLUME_FACTOR_KEY =
        PREFIX + "." + PIPELINE_PER_VOLUME_FACTOR_KEY;

    @Config(key = PIPELINE_PER_VOLUME_FACTOR_KEY,
        type = ConfigType.DOUBLE,
        defaultValue = PIPELINE_PER_VOLUME_FACTOR_DEFAULT_VALUE,
        tags = {SCM},
        description = "TODO"
    )
    private double pipelinePerVolumeFactor = PIPELINE_PER_VOLUME_FACTOR_DEFAULT;

    public double getPipelinePerVolumeFactor() {
      return pipelinePerVolumeFactor;
    }

    @PostConstruct
    public void validate() {
      if (pipelinePerVolumeFactor < 0) {
        LOG.warn("{} must be non-negative, but was {}. Defaulting to {}",
            EC_PIPELINE_PER_VOLUME_FACTOR_KEY, pipelinePerVolumeFactor,
            PIPELINE_PER_VOLUME_FACTOR_DEFAULT);
        pipelinePerVolumeFactor = PIPELINE_PER_VOLUME_FACTOR_DEFAULT;
      }
    }

    public void setPipelinePerVolumeFactor(double v) {
      pipelinePerVolumeFactor = v;
    }
  }

}
