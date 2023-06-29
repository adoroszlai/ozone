/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.MiniOzoneHAClusterImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.ratis.util.function.CheckedConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

/**
 * Base class for tests where cluster initially has 2 active and 1 inactive OMs.
 */
public abstract class TwoActiveOMsCluster {

  private MiniOzoneHAClusterImpl cluster;
  private OzoneClient client;
  private OzoneVolume volume;
  private OzoneBucket bucket;
  private ObjectStore objectStore;

  /**
   * Create a MiniOzoneCluster with 2 active and 1 inactive OMs.
   */
  @BeforeEach
  void init() throws Exception {
    OzoneConfiguration conf = createConfig();
    cluster = (MiniOzoneHAClusterImpl) MiniOzoneCluster.newOMHABuilder(conf)
        .setNumOfOzoneManagers(3)
        .setNumOfActiveOMs(2)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();

    String volumeName = "volume";
    objectStore.createVolume(volumeName);
    volume = objectStore.getVolume(volumeName);

    String bucketName = "bucket";
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
  }

  @AfterEach
  void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  // hook method for customizing config
  protected OzoneConfiguration createConfig() {
    return new OzoneConfiguration();
  }

  public MiniOzoneHAClusterImpl getCluster() {
    return cluster;
  }

  protected ObjectStore getObjectStore() {
    return objectStore;
  }

  protected OzoneClient getClient() {
    return client;
  }

  protected OzoneVolume getVolume() {
    return volume;
  }

  protected OzoneBucket getBucket() {
    return bucket;
  }
}
