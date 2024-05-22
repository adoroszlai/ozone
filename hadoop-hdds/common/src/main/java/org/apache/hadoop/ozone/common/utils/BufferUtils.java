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

package org.apache.hadoop.ozone.common.utils;

import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import jakarta.annotation.Nullable;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.utils.LeakDetector;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for buffers.
 */
public final class BufferUtils {

  private static final LeakDetector LEAK_DETECTOR = new LeakDetector("ChunkBuffer");

  private static final Logger LOG = LoggerFactory.getLogger(BufferUtils.class);

  /** Utility classes should not be constructed. **/
  private BufferUtils() {

  }

  /**
   * Assign an array of ByteBuffers.
   * @param totalLen total length of all ByteBuffers
   * @param bufferCapacity max capacity of each ByteBuffer
   */
  public static ByteBuffer[] assignByteBuffers(long totalLen,
      int bufferCapacity) {
    Preconditions.checkArgument(totalLen > 0, "Buffer Length should be a " +
        "positive integer.");
    Preconditions.checkArgument(bufferCapacity > 0, "Buffer Capacity should " +
        "be a positive integer.");

    int numBuffers = getNumberOfBins(totalLen, bufferCapacity);

    ByteBuffer[] dataBuffers = new ByteBuffer[numBuffers];
    long allocatedLen = 0;
    // For each ByteBuffer (except the last) allocate bufferLen of capacity
    for (int i = 0; i < numBuffers - 1; i++) {
      dataBuffers[i] = ByteBuffer.allocate(bufferCapacity);
      allocatedLen += bufferCapacity;
    }
    // For the last ByteBuffer, allocate as much space as is needed to fit
    // remaining bytes
    dataBuffers[numBuffers - 1] = ByteBuffer.allocate(
        Math.toIntExact(totalLen - allocatedLen));
    return dataBuffers;
  }

  /**
   * Return a read only ByteBuffer list for the input ByteStrings list.
   */
  public static List<ByteBuffer> getReadOnlyByteBuffers(
      List<ByteString> byteStrings) {
    List<ByteBuffer> buffers = new ArrayList<>();
    for (ByteString byteString : byteStrings) {
      buffers.add(byteString.asReadOnlyByteBuffer());
    }
    return buffers;
  }

  /**
   * Return a read only copy of ByteBuffer array.
   */
  public static ByteBuffer[] getReadOnlyByteBuffers(
      ByteBuffer[] byteBuffers) {
    if (byteBuffers == null) {
      return null;
    }
    ByteBuffer[] readOnlyBuffers = new ByteBuffer[byteBuffers.length];
    for (int i = 0; i < byteBuffers.length; i++) {
      readOnlyBuffers[i] = byteBuffers[i] == null ?
          null : byteBuffers[i].asReadOnlyBuffer();
    }
    return readOnlyBuffers;
  }

  /**
   * Return a read only ByteBuffer array for the input ByteStrings list.
   */
  public static ByteBuffer[] getReadOnlyByteBuffersArray(
      List<ByteString> byteStrings) {
    return getReadOnlyByteBuffers(byteStrings).toArray(new ByteBuffer[0]);
  }

  public static ByteString concatByteStrings(List<ByteString> byteStrings) {
    ByteString result = ByteString.EMPTY;
    for (ByteString byteString : byteStrings) {
      result = result.concat(byteString);
    }
    return result;
  }

  /**
   * Return the summation of the length of all ByteStrings.
   */
  public static long getBuffersLen(List<ByteString> buffers) {
    long length = 0;
    for (ByteString buffer : buffers) {
      length += buffer.size();
    }
    return length;
  }

  /**
   * Return the number of bins required to hold all the elements given a max
   * capacity for each bin.
   * @param numElements total number of elements to put in bin
   * @param maxElementsPerBin max number of elements per bin
   * @return number of bins
   */
  public static int getNumberOfBins(long numElements, int maxElementsPerBin) {
    Preconditions.checkArgument(numElements >= 0);
    Preconditions.checkArgument(maxElementsPerBin > 0);
    final long n = 1 + (numElements - 1) / maxElementsPerBin;
    if (n > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Integer overflow: n = " + n
          + " > Integer.MAX_VALUE = " + Integer.MAX_VALUE
          + ", numElements = " + numElements
          + ", maxElementsPerBin = " + maxElementsPerBin);
    }
    return Math.toIntExact(n);
  }

  public static UncheckedAutoCloseable track(AutoCloseable object) {
    final Class<?> clazz = object.getClass();
    final StackTraceElement[] stackTrace = getStackTrace();
    return LEAK_DETECTOR.track(object, () -> reportLeak(clazz, formatStackTrace(stackTrace)));
  }

  private static void reportLeak(Class<?> clazz, String stackTrace) {
    String warning = String.format("%s is not closed properly", clazz.getSimpleName());
    if (stackTrace != null && LOG.isDebugEnabled()) {
      String debugMessage = String.format("%nStackTrace for unclosed instance: %s", stackTrace);
      warning = warning.concat(debugMessage);
    }
    LOG.warn(warning);
  }

  private static @Nullable StackTraceElement[] getStackTrace() {
    return HddsUtils.getStackTrace(LOG);
  }

  private static String formatStackTrace(@Nullable StackTraceElement[] elements) {
    return HddsUtils.formatStackTrace(elements, 4);
  }
}
