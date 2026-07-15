/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.shell;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServerConfig;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * This class tests Ozone sh shell command with FollowerRead.
 * Inspired by TestS3Shell
 */
public abstract class TestOzoneShellHAWithFollowerRead extends TestOzoneShellHA {

  @BeforeAll
  void enableFollowerRead() {
    for (OzoneManager om : cluster().getOzoneManagersList()) {
      enableFollowerRead(true, om);
    }
  }

  @AfterAll
  void disableFollowerRead() {
    for (OzoneManager om : cluster().getOzoneManagersList()) {
      enableFollowerRead(false, om);
    }
  }

  public void enableFollowerRead(boolean enable, OzoneManager om) {
    OzoneConfiguration conf = om.getConfiguration();
    OzoneManagerRatisServerConfig omHAConfig = conf.getObject(OzoneManagerRatisServerConfig.class);
    RaftServerConfigKeys.Read.Option readOption = enable
        ? RaftServerConfigKeys.Read.Option.LINEARIZABLE
        : RaftServerConfigKeys.Read.Option.DEFAULT;
    omHAConfig.setReadOption(readOption.name());
    omHAConfig.setReadLeaderLeaseEnabled(enable);
    conf.setFromObject(omHAConfig);
    conf.setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, enable);

    om.getConfig().setAllowLeaderSkipLinearizableRead(enable);
  }

  @Test
  public void testAllowLeaderSkipLinearizableRead() throws Exception {
    OzoneManager omLeader = cluster().getOMLeader();
    OmConfig oldConf = omLeader.getConfig().copy();
    try {
      String[] args = new String[]{"volume", "list"};
      getOzoneShell().getOzoneConf().setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
      for (int i = 0; i < 100; i++) {
        execute(getOzoneShell(), args);
      }
      long lastMetrics = omLeader.getMetrics().getNumLeaderSkipLinearizableRead();
      assertThat(lastMetrics).isGreaterThan(0);

      omLeader.getConfig().setAllowLeaderSkipLinearizableRead(false);
      for (int i = 0; i < 100; i++) {
        execute(getOzoneShell(), args);
      }
      long curMetrics = omLeader.getMetrics().getNumLeaderSkipLinearizableRead();
      assertEquals(lastMetrics, curMetrics);
    } finally {
      restoreConfig(omLeader, oldConf);
    }
  }

  @Test
  public void testAllowFollowerReadLocalLease() throws Exception {
    OmConfig oldConfig1 = null;
    OmConfig oldConfig2 = null;
    OzoneManager omFollower1 = null;
    OzoneManager omFollower2 = null;
    try {
      for (OzoneManager om : cluster().getOzoneManagersList()) {
        // Leader ignores the local lease and serve the read request
        // immediately so we should test on followers instead
        if (!om.isLeaderReady()) {
          if (omFollower1 == null) {
            omFollower1 = om;
          } else {
            omFollower2 = om;
            break;
          }
        }
      }
      assertNotNull(omFollower1, "Cannot find OM follower");
      assertNotNull(omFollower2, "Cannot find OM follower");
      oldConfig1 = omFollower1.getConfig().copy();
      omFollower1.getConfig().setFollowerReadLocalLeaseEnabled(true);
      // All local lease should fail since the lease time is negative
      oldConfig2 = omFollower2.getConfig().copy();
      omFollower2.getConfig().setFollowerReadLocalLeaseEnabled(true);
      omFollower2.getConfig().setFollowerReadLocalLeaseTimeMs(-1000);

      String[] args = new String[]{"volume", "list"};
      getOzoneShell().getOzoneConf().setBoolean(OZONE_CLIENT_FOLLOWER_READ_ENABLED_KEY, true);
      getOzoneShell().getOzoneConf().set("ozone.client.follower.read.default.consistency", "LOCAL_LEASE");
      for (int i = 0; i < 100; i++) {
        execute(getOzoneShell(), args);
      }
      assertThat(omFollower1.getMetrics().getNumFollowerReadLocalLeaseSuccess()).isPositive();
      // Local lease time is set to negative, for this OM should fail all local lease read requests
      assertEquals(0, omFollower2.getMetrics().getNumFollowerReadLocalLeaseSuccess());
      assertThat(omFollower2.getMetrics().getNumFollowerReadLocalLeaseFailTime()).isPositive();

      // Setting the local lease time and log limit to -1 allow infinite lag
      omFollower2.getConfig().setFollowerReadLocalLeaseTimeMs(-1);
      omFollower2.getConfig().setFollowerReadLocalLeaseLogLimit(-1);
      for (int i = 0; i < 100; i++) {
        execute(getOzoneShell(), args);
      }
      assertThat(omFollower2.getMetrics().getNumFollowerReadLocalLeaseSuccess()).isPositive();
    } finally {
      restoreConfig(omFollower1, oldConfig1);
      restoreConfig(omFollower2, oldConfig2);
    }
  }

  private static void restoreConfig(OzoneManager om, OmConfig config) {
    if (om != null && config != null) {
      om.getConfig().setFrom(config);
    }
  }
}
