/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.interfaces.Handler;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.hadoop.ozone.client.rpc.TestECKeyOutputStream.initConf;
import static org.apache.hadoop.ozone.client.rpc.TestECKeyOutputStream.bucketName;
import static org.apache.hadoop.ozone.client.rpc.TestECKeyOutputStream.inputSize;
import static org.apache.hadoop.ozone.client.rpc.TestECKeyOutputStream.keyString;
import static org.apache.hadoop.ozone.client.rpc.TestECKeyOutputStream.volumeName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;

class TestECKeyCreationWithDatanodeIdChange {

  @Test
  void test() throws Exception {
    AtomicReference<Boolean> failed = new AtomicReference<>(false);
    AtomicReference<MiniOzoneCluster> miniOzoneCluster = new AtomicReference<>();
    OzoneClient client1 = null;
    try (MockedStatic<Handler> mockedHandler = Mockito.mockStatic(Handler.class, Mockito.CALLS_REAL_METHODS)) {
      Map<String, Handler> handlers = new HashMap<>();
      mockedHandler.when(() -> Handler.getHandlerForContainerType(any(), any(), any(), any(), any(), any(), any()))
          .thenAnswer(i -> {
            Handler handler = Mockito.spy((Handler) i.callRealMethod());
            handlers.put(handler.getDatanodeId(), handler);
            return handler;
          });
      OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
      initConf(ozoneConfiguration);
      miniOzoneCluster.set(MiniOzoneCluster.newBuilder(ozoneConfiguration).setNumDatanodes(10).build());
      miniOzoneCluster.get().waitForClusterToBeReady();
      client1 = miniOzoneCluster.get().newClient();
      ObjectStore store = client1.getObjectStore();
      store.createVolume(volumeName);
      store.getVolume(volumeName).createBucket(bucketName);
      OzoneOutputStream key = TestHelper.createKey(keyString, new ECReplicationConfig(3, 2,
          ECReplicationConfig.EcCodec.RS, 1024), inputSize, store, volumeName, bucketName);
      byte[] b = new byte[6 * 1024];
      ECKeyOutputStream groupOutputStream = (ECKeyOutputStream) key.getOutputStream();
      List<OmKeyLocationInfo> locationInfoList = groupOutputStream.getLocationInfoList();
      while (locationInfoList.isEmpty()) {
        locationInfoList = groupOutputStream.getLocationInfoList();
        Random random = new Random();
        random.nextBytes(b);
        assertInstanceOf(ECKeyOutputStream.class, key.getOutputStream());
        key.write(b);
        key.flush();
      }

      assertEquals(1, locationInfoList.size());

      OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);
      long containerId = omKeyLocationInfo.getContainerID();
      Pipeline pipeline = omKeyLocationInfo.getPipeline();
      DatanodeDetails dnWithReplicaIndex1 =
          pipeline.getReplicaIndexes().entrySet().stream().filter(e -> e.getValue() == 1).map(Map.Entry::getKey)
              .findFirst().get();
      Mockito.when(handlers.get(dnWithReplicaIndex1.getUuidString()).getDatanodeId())
          .thenAnswer(i -> {
            if (!failed.get()) {
              // Change dnId for one write chunk request.
              failed.set(true);
              return dnWithReplicaIndex1.getUuidString() + "_failed";
            } else {
              return dnWithReplicaIndex1.getUuidString();
            }
          });
      locationInfoList = groupOutputStream.getLocationInfoList();
      while (locationInfoList.size() == 1) {
        locationInfoList = groupOutputStream.getLocationInfoList();
        Random random = new Random();
        random.nextBytes(b);
        assertInstanceOf(ECKeyOutputStream.class, key.getOutputStream());
        key.write(b);
        key.flush();
      }
      assertEquals(2, locationInfoList.size());
      assertNotEquals(locationInfoList.get(1).getPipeline().getId(), pipeline.getId());
      GenericTestUtils.waitFor(() -> {
        try {
          return miniOzoneCluster.get().getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueOf(containerId)).getState().equals(
                  HddsProtos.LifeCycleState.CLOSED);
        } catch (ContainerNotFoundException e) {
          throw new RuntimeException(e);
        }
      }, 1000, 30000);
      key.close();
      assertTrue(failed.get());
    } finally {
      IOUtils.closeQuietly(client1);
      if (miniOzoneCluster.get() != null) {
        miniOzoneCluster.get().shutdown();
      }
    }
  }

}
