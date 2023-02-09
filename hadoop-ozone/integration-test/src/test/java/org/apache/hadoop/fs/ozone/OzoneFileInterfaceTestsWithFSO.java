/*
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

package org.apache.hadoop.fs.ozone;

import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;

/**
 * Test OzoneFileSystem Interfaces - prefix layout.
 *
 * This test will test the various interfaces i.e.
 * create, read, write, getFileStatus
 */
abstract class OzoneFileInterfaceTestsWithFSO extends OzoneFileInterfaceTests {

  static class TestFSODefaultFS extends OzoneFileInterfaceTestsWithFSO {
    TestFSODefaultFS() {
      super(true, false, false);
    }
  }

  static class TestFSOAbsolutePath extends OzoneFileInterfaceTestsWithFSO {
    TestFSOAbsolutePath() {
      super(false, true, false);
    }
  }
  OzoneFileInterfaceTestsWithFSO(boolean setDefaultFs,
      boolean useAbsolutePath, boolean enabledFileSystemPaths) {
    super(setDefaultFs, useAbsolutePath, enabledFileSystemPaths);
  }

  @Override
  @Test
  @Disabled("HDDS-2939")
  public void testReplication() throws IOException {
    // ignore as this is not relevant to PREFIX layout changes
  }

  @Override
  @Test
  @Disabled("HDDS-2939")
  public void testPathToKey() throws Exception {
    // ignore as this is not relevant to PREFIX layout changes
  }

  @Override
  @Test
  @Disabled("HDDS-2939")
  public void testFileSystemInit() throws IOException {
    // ignore as this is not relevant to PREFIX layout changes
  }

  @Override
  @Test
  @Disabled("TODO:HDDS-2939")
  public void testDirectory() {

  }

  @Override
  @Test
  @Disabled("TODO:HDDS-2939")
  public void testOzFsReadWrite() {

  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
