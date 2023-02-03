package org.apache.hadoop.ozone.reconfig;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager.ReplicationManagerConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for OM Reconfigure.
 */
@Timeout(300)
class TestOmReconfigure {

  private static MiniOzoneCluster cluster;

  @BeforeAll
  static void setup() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(new OzoneConfiguration())
        .setStartDataNodes(false)
        .build();
  }

  @AfterAll
  static void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Test reconfigure om "ozone.administrators".
   */
  @Test
  void testOmAdminUsersReconfigure() throws Exception {
    OzoneManager ozoneManager = cluster.getOzoneManager();
    String userA = "mockUserA";
    String userB = "mockUserB";
    ozoneManager.reconfigureProperty(OZONE_ADMINISTRATORS, userA);
    assertTrue(ozoneManager.getOmAdminUsernames().contains(userA),
        userA + " should be an admin user");

    ozoneManager.reconfigureProperty(OZONE_ADMINISTRATORS, userB);
    assertFalse(ozoneManager.getOmAdminUsernames().contains(userA),
        userA + " should NOT be an admin user");
    assertTrue(ozoneManager.getOmAdminUsernames().contains(userB),
        userB + " should be an admin user");
  }

  @Test
  void reconfigurePushReplication() throws Exception {
    StorageContainerManager scm = cluster.getStorageContainerManager();
    ReplicationManagerConfiguration rmConf =
        scm.getReplicationManager().getConfig();
    assertFalse(rmConf.isPush());

    scm.reconfigureProperty("hdds.scm.replication.push", "true");

    assertTrue(rmConf.isPush());
  }

}
