/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.contract.AbstractContractCreateTest;
import org.apache.hadoop.fs.contract.AbstractContractDeleteTest;
import org.apache.hadoop.fs.contract.AbstractContractGetFileStatusTest;
import org.apache.hadoop.fs.contract.AbstractContractMkdirTest;
import org.apache.hadoop.fs.contract.AbstractContractOpenTest;
import org.apache.hadoop.fs.contract.AbstractContractRenameTest;
import org.apache.hadoop.fs.contract.AbstractContractRootDirectoryTest;
import org.apache.hadoop.fs.contract.AbstractContractSeekTest;
import org.apache.hadoop.fs.contract.AbstractContractUnbufferTest;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.hdds.conf.DatanodeRatisServerConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.ratis.conf.RatisClientConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.tools.contract.AbstractContractDistCpTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;
import java.time.Duration;

import static org.apache.hadoop.fs.contract.ContractTestUtils.cleanup;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_HSYNC_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_OFS_URI_SCHEME;
import static org.assertj.core.api.Assumptions.assumeThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class AbstractOzoneContractTest {

  static final String CONTRACT_XML = "contract/ozone.xml";
  private MiniOzoneCluster cluster;

  protected static OzoneConfiguration createBaseConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    DatanodeRatisServerConfig ratisServerConfig =
        conf.getObject(DatanodeRatisServerConfig.class);
    ratisServerConfig.setRequestTimeOut(Duration.ofSeconds(3));
    ratisServerConfig.setWatchTimeOut(Duration.ofSeconds(10));
    conf.setFromObject(ratisServerConfig);

    RatisClientConfig.RaftConfig raftClientConfig =
        conf.getObject(RatisClientConfig.RaftConfig.class);
    raftClientConfig.setRpcRequestTimeout(Duration.ofSeconds(3));
    raftClientConfig.setRpcWatchRequestTimeout(Duration.ofSeconds(10));
    conf.setFromObject(raftClientConfig);

    conf.addResource(CONTRACT_XML);

    conf.setBoolean(OZONE_FS_HSYNC_ENABLED, true);

    return conf;
  }

  abstract OzoneConfiguration createOzoneConfig();

  abstract AbstractFSContract createOzoneContract(Configuration conf);

  public MiniOzoneCluster getCluster() {
    return cluster;
  }

  @BeforeAll
  void setup() throws Exception {
    cluster = MiniOzoneCluster.newBuilder(createOzoneConfig())
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  void teardown() {
    IOUtils.closeQuietly(cluster);
  }

  @Nested
  public class TestContractCreate extends AbstractContractCreateTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  public class TestContractDistCp extends AbstractContractDistCpTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }

    @Override
    protected void deleteTestDirInTeardown() throws IOException {
      super.deleteTestDirInTeardown();
      cleanup("TEARDOWN", getLocalFS(), getLocalDir());
    }
  }

  @Nested
  public class TestContractDelete extends AbstractContractDeleteTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  public class TestContractGetFileStatus extends AbstractContractGetFileStatusTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  public class TestContractMkdir extends AbstractContractMkdirTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  public class TestContractOpen extends AbstractContractOpenTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  public class TestContractRename extends AbstractContractRenameTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  public class TestContractRootDirectory extends AbstractContractRootDirectoryTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }

    @Override
    @Test
    public void testRmRootRecursive() throws Throwable {
      // OFS doesn't support creating files directly under root
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testRmRootRecursive();
    }

    @Override
    @Test
    public void testRmNonEmptyRootDirNonRecursive() throws Throwable {
      // OFS doesn't support creating files directly under root
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testRmNonEmptyRootDirNonRecursive();
    }

    @Override
    @Test
    public void testRmEmptyRootDirNonRecursive() throws Throwable {
      // Internally test deletes volume recursively
      // Which is not supported
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testRmEmptyRootDirNonRecursive();
    }

    @Override
    @Test
    public void testListEmptyRootDirectory() throws IOException {
      // Internally test deletes volume recursively
      // Which is not supported
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testListEmptyRootDirectory();
    }

    @Override
    @Test
    public void testSimpleRootListing() throws IOException {
      // Recursive list is not supported
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testSimpleRootListing();
    }

    @Override
    @Test
    public void testMkDirDepth1() throws Throwable {
      // Internally test deletes volume recursively
      // Which is not supported
      assumeThat(getContract().getScheme())
          .isNotEqualTo(OZONE_OFS_URI_SCHEME);
      super.testMkDirDepth1();
    }
  }

  @Nested
  public class TestContractSeek extends AbstractContractSeekTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

  @Nested
  public class TestContractUnbuffer extends AbstractContractUnbufferTest {
    @Override
    protected Configuration createConfiguration() {
      return createOzoneConfig();
    }

    @Override
    protected AbstractFSContract createContract(Configuration conf) {
      return createOzoneContract(conf);
    }
  }

}
