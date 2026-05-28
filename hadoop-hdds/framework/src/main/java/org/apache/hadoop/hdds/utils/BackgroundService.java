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

package org.apache.hadoop.hdds.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract class for a background service in ozone.
 * A background service schedules multiple child tasks in parallel
 * in a certain period. In each interval, it waits until all the tasks
 * finish execution and then schedule next interval.
 */
public abstract class BackgroundService {

  protected static final Logger LOG =
      LoggerFactory.getLogger(BackgroundService.class);

  private final Lock lock = new ReentrantLock(); // protects executor creation/shutdown
  // Executor to launch child tasks
  private ScheduledThreadPoolExecutor exec;
  private ThreadGroup threadGroup;
  private final String serviceName;
  private long interval;
  private long serviceTimeoutInNanos;
  private final TimeUnit unit;
  private int threadPoolSize;
  private final String threadNamePrefix;
  private final PeriodicalTask service;
  private CompletableFuture<Void> future;

  public BackgroundService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout) {
    this(serviceName, interval, unit, threadPoolSize, serviceTimeout, "");
  }

  public BackgroundService(String serviceName, long interval,
      TimeUnit unit, int threadPoolSize, long serviceTimeout,
      String threadNamePrefix) {
    this.interval = interval;
    this.unit = unit;
    this.serviceName = serviceName;
    this.serviceTimeoutInNanos = TimeDuration.valueOf(serviceTimeout, unit)
            .toLong(TimeUnit.NANOSECONDS);
    this.threadPoolSize = threadPoolSize;
    this.threadNamePrefix = threadNamePrefix;
    initExecutorAndThreadGroup();
    service = new PeriodicalTask();
    this.future = CompletableFuture.completedFuture(null);
  }

  protected Lock getLock() {
    return lock;
  }

  protected CompletableFuture<Void> getFuture() {
    return future;
  }

  @VisibleForTesting
  protected ExecutorService getExecutorService() {
    return this.exec;
  }

  protected void setPoolSize(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("Pool size must be positive.");
    }

    lock.lock();
    try {
      threadPoolSize = size;
      exec.setCorePoolSize(size);
    } finally {
      lock.unlock();
    }
  }

  protected void setServiceTimeoutInNanos(long newTimeout) {
    LOG.info("{} timeout is set to {}ns", serviceName, newTimeout);
    this.serviceTimeoutInNanos = newTimeout;
  }

  @VisibleForTesting
  public int getThreadCount() {
    return threadGroup.activeCount();
  }

  @VisibleForTesting
  public void runPeriodicalTaskNow() throws Exception {
    BackgroundTaskQueue tasks = getTasks();
    while (!tasks.isEmpty()) {
      tasks.poll().call();
    }
    execTaskCompletion();
  }

  // start service
  public void start() {
    final long currentInterval = interval;
    LOG.info("Starting service {} with interval {} {}", serviceName,
        currentInterval, unit.name().toLowerCase());
    lock.lock();
    try {
      if (exec == null || exec.isShutdown() || exec.isTerminated()) {
        initExecutorAndThreadGroup();
      }
      exec.scheduleWithFixedDelay(service, 0, currentInterval, unit);
    } finally {
      lock.unlock();
    }
  }

  /** @param hook to be run between shutdown and start */
  protected void restart(long newInterval, int newCorePoolSize, Runnable hook) {
    if (newCorePoolSize <= 0) {
      throw new IllegalArgumentException("Pool size must be positive.");
    }

    lock.lock();
    try {
      shutdown();
      interval = newInterval;
      threadPoolSize = newCorePoolSize;
      hook.run();
      start();
    } finally {
      lock.unlock();
    }
  }

  public TimeUnit getTimeUnit() {
    return unit;
  }

  protected long getIntervalMillis() {
    return this.unit.toMillis(interval);
  }

  public abstract BackgroundTaskQueue getTasks();

  protected void execTaskCompletion() { }

  /**
   * Run one or more background tasks concurrently.
   * Wait until all tasks to return the result.
   */
  public class PeriodicalTask implements Runnable {
    @Override
    public void run() {
      // wait for previous set of tasks to complete
      try {
        future.join();
      } catch (RuntimeException e) {
        LOG.error("Background service execution failed.", e);
      } finally {
        execTaskCompletion();
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Running background service : {}", serviceName);
      }
      BackgroundTaskQueue tasks = getTasks();
      if (tasks.isEmpty()) {
        // No task found, or some problems to init tasks
        // return and retry in next interval.
        return;
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Number of background tasks to execute : {}", tasks.size());
      }
      try {
        final long threshold = serviceTimeoutInNanos;
        if (lock.tryLock(threshold, TimeUnit.NANOSECONDS)) {
          try {
            while (!tasks.isEmpty()) {
              BackgroundTask task = tasks.poll();
              future = future.thenCombine(CompletableFuture.runAsync(() -> {
                long startTime = System.nanoTime();
                try {
                  BackgroundTaskResult result = task.call();
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("task execution result size {}", result.getSize());
                  }
                } catch (Throwable e) {
                  LOG.error("Background task execution failed", e);
                  if (e instanceof Error) {
                    throw (Error) e;
                  }
                } finally {
                  long endTime = System.nanoTime();
                  if (endTime - startTime > threshold) {
                    LOG.warn("{} Background task execution took {}ns > {}ns(timeout)",
                        serviceName, endTime - startTime, threshold);
                  }
                }
              }, exec).exceptionally(e -> null), (Void1, Void) -> null);
            }
          } finally {
            lock.unlock();
          }
        }
      } catch (InterruptedException e) {
        // if lock is already taken, startup/shutdown is in progress
        LOG.info("{} submission timeout", serviceName);
      }
    }
  }

  // shutdown and make sure all threads are properly released.
  public void shutdown() {
    LOG.info("Shutting down service {}", this.serviceName);
    lock.lock();
    try {
      exec.shutdown();
      if (!exec.awaitTermination(60, TimeUnit.SECONDS)) {
        exec.shutdownNow();
      }
    } catch (InterruptedException e) {
      // Re-interrupt the thread while catching InterruptedException
      Thread.currentThread().interrupt();
      exec.shutdownNow();
    } finally {
      lock.unlock();
    }
  }

  private void initExecutorAndThreadGroup() {
    threadGroup = new ThreadGroup(serviceName);
    ThreadFactory threadFactory = new ThreadFactoryBuilder()
        .setThreadFactory(r -> new Thread(threadGroup, r))
        .setDaemon(true)
        .setNameFormat(threadNamePrefix + serviceName + "#%d")
        .build();
    exec = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
  }

  protected String getServiceName() {
    return serviceName;
  }
}
