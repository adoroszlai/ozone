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
package org.apache.hadoop.ozone.om;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.ratis.util.function.CheckedConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Simple test case with default configs.
 */
class TestTwoActiveOMsWithDefaultConfig extends TwoActiveOMsCluster {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestTwoActiveOMsWithDefaultConfig.class);

  private static final byte[] content = RandomStringUtils.randomAscii(100)
      .getBytes(StandardCharsets.UTF_8);

  @ParameterizedTest
  @ValueSource(ints = {100, 1000})
  void createBuckets(int count) throws IOException {
    String prefix = UUID.randomUUID().toString();
    for (int i = 0; i < count; i++) {
      getVolume().createBucket(prefix + i);
    }
  }

  @Test
  void createEmptyKeys() throws IOException {
    createKeys(10000, name -> getBucket().createKey(name, 0).close());
  }

  @Test
  void createKeys() throws IOException {
    createKeys(1000, name -> {
      try (OutputStream out = getBucket().createKey(name, 0)) {
        out.write(content);
      }
    });
  }

  private void createKeys(int count,
      CheckedConsumer<String, IOException> creator) throws IOException {
    final int percent = count / 100;
    final String prefix = UUID.randomUUID().toString();

    for (int i = 0; i < count; i++) {
      if (percent > 0 && i % percent == 0) {
        LOG.info("{}% of {} keys", i / percent, count);
      }
      creator.accept(prefix + "-" + i);
    }
    LOG.info("100% of {} keys", count);
  }
}
