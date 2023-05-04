/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.container.common;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.container.common.utils.DatanodeStoreCache;
import org.apache.hadoop.ozone.container.common.utils.RawDB;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test DatanodeStoreCache.
 */
public class TestDatanodeStoreCache {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  private OzoneConfiguration conf = new OzoneConfiguration();

  @Test
  public void testBasicOperations() throws IOException {
    DatanodeStoreCache cache = DatanodeStoreCache.getInstance();
    String dbPath1 = folder.newFolder("basic1").getAbsolutePath();
    String dbPath2 = folder.newFolder("basic2").getAbsolutePath();
    DatanodeStore store1 = new DatanodeStoreSchemaThreeImpl(conf, dbPath1,
        false);
    DatanodeStore store2 = new DatanodeStoreSchemaThreeImpl(conf, dbPath2,
        false);

    // test normal add
    RawDB db1 = new RawDB(store1, dbPath1);
    assertTrue(cache.addDB(dbPath1, db1));
    assertTrue(cache.addDB(dbPath2, new RawDB(store2, dbPath2)));
    assertEquals(2, cache.size());

    // test duplicate add
    assertFalse(cache.addDB(dbPath1, new RawDB(store1, dbPath1)));
    assertEquals(2, cache.size());

    // test get, test reference the same object using ==
    RawDB db = cache.getDB(dbPath1, conf);
    assertSame(db1, db);
    assertSame(store1, db.getStore());

    // test remove
    cache.removeDB(dbPath1);
    assertEquals(1, cache.size());

    // test remove non-exist should not throw
    cache.removeDB(dbPath1);

    // test shutdown
    cache.shutdownCache();
    assertEquals(0, cache.size());
  }
}
