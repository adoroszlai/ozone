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
    OzoneConfiguration oldConf = cluster().getConf();
    OzoneManager omLeader = cluster().getOMLeader();
    try {
      String[] args = new String[]{"volume", "list"};
      getOzoneShell().getOzoneConf().setBoolean("ozone.client.follower.read.enabled", true);
      for (int i = 0; i < 100; i++) {
        execute(getOzoneShell(), args);
      }
      long lastMetrics = omLeader.getMetrics().getNumLeaderSkipLinearizableRead();
      assertThat(lastMetrics).isGreaterThan(0);
      OzoneConfiguration newConf = new OzoneConfiguration(oldConf);
      newConf.setBoolean("ozone.om.allow.leader.skip.linearizable.read", false);
      omLeader.setConfiguration(newConf);
      for (int i = 0; i < 100; i++) {
        execute(getOzoneShell(), args);
      }
      long curMetrics = omLeader.getMetrics().getNumLeaderSkipLinearizableRead();
      assertEquals(lastMetrics, curMetrics);
    } finally {
      omLeader.setConfiguration(oldConf);
    }
  }

  @Test
  public void testAllowFollowerReadLocalLease() throws Exception {
    OzoneConfiguration oldConf = cluster().getConf();
    OzoneConfiguration newConf1 = new OzoneConfiguration(oldConf);
    newConf1.setBoolean("ozone.om.follower.read.local.lease.enabled", true);
    OzoneConfiguration newConf2 = new OzoneConfiguration(newConf1);
    // All local lease should fail since the lease time is negative
    newConf2.setLong("ozone.om.follower.read.local.lease.time.ms", -1000);
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
      omFollower1.setConfiguration(newConf1);
      omFollower2.setConfiguration(newConf2);

      String[] args = new String[]{"volume", "list"};
      getOzoneShell().getOzoneConf().setBoolean("ozone.client.follower.read.enabled", true);
      getOzoneShell().getOzoneConf().set("ozone.client.follower.read.default.consistency", "LOCAL_LEASE");
      for (int i = 0; i < 100; i++) {
        execute(getOzoneShell(), args);
      }
      assertThat(omFollower1.getMetrics().getNumFollowerReadLocalLeaseSuccess() > 0).isTrue();
      // Local lease time is set to negative, for this OM should fail all local lease read requests
      assertEquals(0, omFollower2.getMetrics().getNumFollowerReadLocalLeaseSuccess());
      assertThat(omFollower2.getMetrics().getNumFollowerReadLocalLeaseFailTime() > 0).isTrue();

      // Setting the local lease time and log limit to -1 allow infinite lag
      newConf2.setLong("ozone.om.follower.read.local.lease.time.ms", -1);
      newConf2.setLong("ozone.om.follower.read.local.lease.log.limit", -1);
      omFollower2.setConfiguration(newConf2);
      for (int i = 0; i < 100; i++) {
        execute(getOzoneShell(), args);
      }
      assertThat(omFollower2.getMetrics().getNumFollowerReadLocalLeaseSuccess() > 0).isTrue();
    } finally {
      if (omFollower1 != null) {
        omFollower1.setConfiguration(oldConf);
      }
      if (omFollower2 != null) {
        omFollower2.setConfiguration(oldConf);
      }
    }
  }
}
