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

package org.apache.hadoop.hdds.scm;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.StorageContainerManagerHttpServer;
import org.apache.hadoop.hdds.server.http.HttpConfig;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;

import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test http server os SCM with various HTTP option.
 */
class TestStorageContainerManagerHttpServer {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestStorageContainerManagerHttpServer.class);
  private static final String BASEDIR = GenericTestUtils
      .getTempPath(TestStorageContainerManagerHttpServer.class.getSimpleName());
  private static String keystoresDir;
  private static String sslConfDir;
  private static OzoneConfiguration conf;
  private static URLConnectionFactory connectionFactory;

  @BeforeAll
  static void setUp() throws Exception {
    File base = new File(BASEDIR);
    FileUtil.fullyDelete(base);
    base.mkdirs();
    conf = new OzoneConfiguration();
    keystoresDir = new File(BASEDIR).getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(
        TestStorageContainerManagerHttpServer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    connectionFactory =
        URLConnectionFactory.newDefaultURLConnectionFactory(conf);
    conf.set(OzoneConfigKeys.OZONE_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(OzoneConfigKeys.OZONE_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());
  }

  @AfterAll
  static void tearDown() throws Exception {
    connectionFactory.destroy();
    FileUtil.fullyDelete(new File(BASEDIR));
    KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
  }

  //@ParameterizedTest
  //@EnumSource(HttpConfig.Policy.class)
  //void testHttpPolicy(HttpConfig.Policy policy) throws Exception {
  @RepeatedTest(50)
  void testHttpPolicy() throws Exception {
    HttpConfig.Policy policy = HttpConfig.Policy.HTTP_ONLY;
    conf.set(OzoneConfigKeys.OZONE_HTTP_POLICY_KEY, policy.name());
    conf.set(ScmConfigKeys.OZONE_SCM_HTTP_ADDRESS_KEY, "localhost:0");
    conf.set(ScmConfigKeys.OZONE_SCM_HTTPS_ADDRESS_KEY, "localhost:0");

    StorageContainerManagerHttpServer server = null;
    try {
      DefaultMetricsSystem.initialize("TestStorageContainerManagerHttpServer");
      server = new StorageContainerManagerHttpServer(conf, null);
      server.start();

      Assert.assertTrue(implies(policy.isHttpEnabled(),
          canAccess("http", server.getHttpAddress())));
      Assert.assertTrue(implies(policy.isHttpEnabled() &&
              !policy.isHttpsEnabled(),
          !canAccess("https", server.getHttpsAddress())));

      Assert.assertTrue(implies(policy.isHttpsEnabled(),
          canAccess("https", server.getHttpsAddress())));
      Assert.assertTrue(implies(policy.isHttpsEnabled() &&
              !policy.isHttpEnabled(),
          !canAccess("http", server.getHttpAddress())));

    } finally {
      if (server != null) {
        server.stop();
      }
    }
  }

  private static boolean canAccess(String scheme, InetSocketAddress addr) {
    if (addr == null) {
      return false;
    }
    String url = scheme + "://" + NetUtils.getHostPortString(addr) + "/jmx";
    try {
      URLConnection conn = connectionFactory.openConnection(new URL(url));
      conn.connect();
      conn.getContent();
    } catch (IOException e) {
      LOG.info("Cannot access {}", url, e);
      return false;
    }
    LOG.info("Can access {}", url);
    return true;
  }

  private static boolean implies(boolean a, boolean b) {
    return !a || b;
  }
}
