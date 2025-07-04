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

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.Nested;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.*;
import static org.apache.hadoop.fs.ozone.AbstractOzoneFileSystemTest.TRASH_INTERVAL;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

public class TestOzoneFileSystem extends ClusterForTests<MiniOzoneCluster> {

  @Override
  protected OzoneConfiguration createOzoneConfig() {
    OzoneConfiguration conf = super.createOzoneConfig();
    conf.setFloat(OMConfigKeys.OZONE_FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_INTERVAL_KEY, TRASH_INTERVAL);
    conf.setFloat(FS_TRASH_CHECKPOINT_INTERVAL_KEY, TRASH_INTERVAL / 2);
    conf.setInt(OmConfig.Keys.SERVER_LIST_MAX_SIZE, 2);
    // set OFS as the default filesystem in the cluster for safety (avoid fallback to local filesystem)
    conf.set(FS_DEFAULT_NAME_KEY, String.format("%s:///", OzoneConsts.OZONE_OFS_URI_SCHEME));
    return conf;
  }

  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return MiniOzoneCluster.newBuilder(createOzoneConfig())
        .setNumDatanodes(5)
        .build();
  }

  @Nested
  class O3FS extends TestO3FS {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class O3FSWithFSO extends TestO3FSWithFSO {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }

  @Nested
  class O3FSWithFSPaths extends TestO3FSWithFSPaths {
    @Override
    public MiniOzoneCluster cluster() {
      return getCluster();
    }
  }
}
