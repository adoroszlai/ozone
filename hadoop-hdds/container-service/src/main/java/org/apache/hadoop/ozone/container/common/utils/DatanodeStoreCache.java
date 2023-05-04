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
package org.apache.hadoop.ozone.container.common.utils;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.ozone.container.metadata.DatanodeStore;
import org.apache.hadoop.ozone.container.metadata.DatanodeStoreSchemaThreeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Cache for all per-disk DB handles under schema v3.
 */
public final class DatanodeStoreCache {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeStoreCache.class);

  /**
   * Use container db absolute path as key.
   */
  private final Map<String, RawDB> datanodeStoreMap;

  private static DatanodeStoreCache cache;

  private DatanodeStoreCache() {
    datanodeStoreMap = Collections.synchronizedMap(new HashMap<>());
  }

  public static synchronized DatanodeStoreCache getInstance() {
    if (cache == null) {
      cache = new DatanodeStoreCache();
    }
    return cache;
  }

  public boolean addDB(String containerDBPath, RawDB db) {
    final RawDB previous;
    synchronized (datanodeStoreMap) {
      previous = datanodeStoreMap.putIfAbsent(containerDBPath, db);
    }

    if (previous == null) {
      LOG.info("Added db {} to cache", containerDBPath);
    } else {
      LOG.info("db {} already cached", containerDBPath);
    }

    return previous == null;
  }

  public RawDB getDB(String containerDBPath, ConfigurationSource conf)
      throws IOException {
    RawDB db;

    synchronized (datanodeStoreMap) {
      db = datanodeStoreMap.get(containerDBPath);
      if (db != null) {
        return db;
      }

      try {
        DatanodeStore store = new DatanodeStoreSchemaThreeImpl(
            conf, containerDBPath, false);
        db = new RawDB(store, containerDBPath);
        datanodeStoreMap.put(containerDBPath, db);
      } catch (IOException e) {
        LOG.error("Failed to get DB store {}", containerDBPath, e);
        throw new IOException("Failed to get DB store " +
            containerDBPath, e);
      }
    }

    LOG.info("Added db {} to cache", containerDBPath);
    return db;
  }

  public void removeDB(String containerDBPath) {
    final RawDB db;
    synchronized (datanodeStoreMap) {
      db = datanodeStoreMap.remove(containerDBPath);
    }

    if (db == null) {
      LOG.debug("DB {} already removed", containerDBPath);
      return;
    }

    try {
      db.getStore().stop();
    } catch (Exception e) {
      LOG.error("Stop DatanodeStore: {} failed", containerDBPath, e);
    }
    LOG.info("Removed db {} from cache", containerDBPath);
  }

  public void shutdownCache() {
    final Map<String, RawDB> copy;
    synchronized (datanodeStoreMap) {
      copy = new TreeMap<>(datanodeStoreMap);
      datanodeStoreMap.clear();
    }

    for (Map.Entry<String, RawDB> entry : copy.entrySet()) {
      try {
        entry.getValue().getStore().stop();
      } catch (Exception e) {
        LOG.warn("Stop DatanodeStore: {} failed", entry.getKey(), e);
      }
    }
  }

  public int size() {
    return datanodeStoreMap.size();
  }
}
