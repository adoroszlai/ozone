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
package org.apache.ozone;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestInterrupt {

  private static final Logger LOG = LoggerFactory.getLogger(
      TestInterrupt.class);

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  @Timeout(60)
  void interruptBeforeSleep(boolean waitUntilAlive) throws InterruptedException {
    final AtomicBoolean interrupted = new AtomicBoolean();
    final AtomicBoolean wasInterrupted = new AtomicBoolean();
    final AtomicInteger iterations = new AtomicInteger();
    Thread thread = new Thread(() -> {
      LOG.info("starting");
      while (!interrupted.get()) {
        LOG.info("not interrupted yet");
      }
      boolean interruptedBeforeSleep = Thread.currentThread().isInterrupted();
      wasInterrupted.set(interruptedBeforeSleep);
      LOG.info("before sleep; was interrupted: " + interruptedBeforeSleep);
      try {
        int i = 0;
        for (; i < 100 && !Thread.currentThread().isInterrupted(); i++) {
          Thread.sleep(100);
        }
        iterations.set(i);
        LOG.info("sleep over after " + i + " attempts");
      } catch (InterruptedException e) {
        LOG.info("sleep interrupted");
      }
    });
    thread.setName("otherThread");
    Thread interrupter = new Thread(() -> {
      LOG.info("starting");
      while (waitUntilAlive && !thread.isAlive()) {
        LOG.info("other thread not alive yet");
      }
      LOG.info("about to interrupt");
      thread.interrupt();
      LOG.info("interrupted other thread");
      interrupted.set(true);
      LOG.info("marked as interrupted");
    });
    interrupter.setName("interruptor");

    thread.start();
    interrupter.start();
    interrupter.join();
    thread.join();

    assertTrue(interrupted.get());
    assertEquals(0, iterations.get());
    assertTrue(wasInterrupted.get());
  }
}
