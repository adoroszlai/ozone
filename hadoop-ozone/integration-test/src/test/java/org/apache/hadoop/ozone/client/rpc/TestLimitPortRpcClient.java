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

package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


/**
 * This class is to test all the public facing APIs of Ozone Client.
 */
@Timeout(300)
public class TestLimitPortRpcClient extends TestOzoneRpcClientAbstract {

  /**
   * Create a MiniOzoneCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    conf.setBoolean(ScmConfigKeys.OZONE_SCM_PIPELINE_AUTO_CREATE_FACTOR_ONE,
        false);
    conf.setBoolean(OMConfigKeys.OZONE_OM_RATIS_ENABLE_KEY, true);
    conf.setBoolean(OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
        true);
    conf.setBoolean(OzoneConfigKeys.OZONE_ACL_ENABLED, true);
    conf.set(OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS,
        OzoneConfigKeys.OZONE_ACL_AUTHORIZER_CLASS_NATIVE);
    conf.setBoolean(OzoneConfigKeys.OZONE_CLIENT_LIMIT_PORT_ENABLE, true);
    startCluster(conf);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() throws IOException {
    shutdownCluster();
  }

  @Test
  public void testCreateKeyLimitPort() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    RpcClient client = new RpcClient(getCluster().getConf(), null);

    try (OzoneOutputStream out = bucket.createKey(keyName, 1)) {
      assertFalse(out.getKeyOutputStream().getStreamEntries().isEmpty());
      for (BlockOutputStreamEntry streamEntry : out.getKeyOutputStream()
          .getStreamEntries()) {
        assertFalse(streamEntry.getPipeline().getNodes().isEmpty());
        for (DatanodeDetails node : streamEntry.getPipeline().getNodes()) {
          Set<Name> ports = node.getPorts().stream().map(Port::getName).collect(
              Collectors.toSet());
          Set<DatanodeDetails.Port.Name> ioPort = client.getIOPorts().stream()
              .map(DatanodeDetails.Port::getFromPortName).collect(
                  Collectors.toSet());
          // Assert that the datanodes only expose the ioPort required by the client
          // when OZONE_CLIENT_LIMIT_PORT_ENABLE is true when `createKey`.
          assertEquals(ioPort, ports);
        }
      }
    }
  }

  @Test
  public void testCreateFileLimitPort() throws IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    getStore().createVolume(volumeName);
    OzoneVolume volume = getStore().getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    RpcClient client = new RpcClient(getCluster().getConf(), null);
    try (OzoneOutputStream out = bucket.createFile(keyName, 1, ReplicationConfig.fromTypeAndFactor(RATIS, THREE),
        true, true)) {
      assertFalse(out.getKeyOutputStream().getStreamEntries().isEmpty());
      for (BlockOutputStreamEntry streamEntry : out.getKeyOutputStream()
          .getStreamEntries()) {
        assertFalse(streamEntry.getPipeline().getNodes().isEmpty());
        for (DatanodeDetails node : streamEntry.getPipeline().getNodes()) {
          Set<Name> ports = node.getPorts().stream().map(Port::getName).collect(
              Collectors.toSet());
          Set<DatanodeDetails.Port.Name> ioPort = client.getIOPorts().stream()
              .map(DatanodeDetails.Port::getFromPortName).collect(
                  Collectors.toSet());
          // Assert that the datanodes only expose the ioPort required by the client
          // when OZONE_CLIENT_LIMIT_PORT_ENABLE is true when `createFile`.
          assertEquals(ioPort, ports);
        }
      }
    }
  }
}
