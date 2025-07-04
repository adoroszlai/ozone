package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.ozone.MiniOzoneCluster;

class TestOzoneFileSystem extends ClusterForFileSystemTests {
  @Override
  protected MiniOzoneCluster createCluster() throws Exception {
    return newClusterBuilder().build();
  }
}
