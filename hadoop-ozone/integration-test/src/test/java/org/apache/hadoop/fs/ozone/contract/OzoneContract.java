/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.fs.ozone.contract;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.Assert;

import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT;

/**
 * FS contract for O3FS.
 */
public class OzoneContract extends AbstractFSContract {

  private final MiniOzoneCluster cluster;

  OzoneContract(MiniOzoneCluster cluster) {
    super(cluster.getConf());
    this.cluster = cluster;
    addConfResource(AbstractOzoneContractTest.CONTRACT_XML);
  }

  @Override
  public String getScheme() {
    return OzoneConsts.OZONE_URI_SCHEME;
  }

  @Override
  public Path getTestPath() {
    return new Path("/test");
  }

  @Override
  public FileSystem getTestFileSystem() throws IOException {
    //assumes cluster is not null
    Assert.assertNotNull("cluster not created", cluster);

    try (OzoneClient client = cluster.newClient()) {
      BucketLayout layout = BucketLayout.fromString(
          getConf().get(OZONE_DEFAULT_BUCKET_LAYOUT, OZONE_DEFAULT_BUCKET_LAYOUT_DEFAULT));
      OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(client, layout);

      String uri = String.format("%s://%s.%s/",
          getScheme(), bucket.getName(), bucket.getVolumeName());
      getConf().set("fs.defaultFS", uri);
    }

    return FileSystem.get(getConf());
  }
}
