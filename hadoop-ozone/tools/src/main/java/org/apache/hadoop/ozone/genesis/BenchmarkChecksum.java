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
package org.apache.hadoop.ozone.genesis;

import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.Java9Crc32CByteBuffer;
import org.apache.hadoop.ozone.common.PureJavaCrc32CByteBuffer;

import org.apache.hadoop.util.Shell;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.hadoop.ozone.common.Checksum.newChecksumByteBufferFunction;

/**
 * Benchmark for checksum implementations (bypassing {@link Checksum} object).
 */
public class BenchmarkChecksum {

  /**
   * State (including parameters) for {@link BenchmarkChecksum}.
   */
  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({"true", "false"})
    private boolean asReadOnly;

    @Param("10")
    private int count;

    @Param({"4194304"})
    private int dataLength;

    @Param({"1024"})
    private int kbPerChecksum;

    @Param({"false"})
    private boolean directBuffer;

    private List<ByteBuffer> buffers;

    @Setup
    public void createData() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      buffers = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        buffers.add(newData(random));
      }
    }

    private ByteBuffer newData(Random random) {
      final byte[] bytes = new byte[dataLength];
      random.nextBytes(bytes);
      final ByteBuffer buffer = allocateByteBuffer(bytes.length);
      buffer.mark();
      buffer.put(bytes);
      buffer.reset();
      return buffer;
    }

    private ByteBuffer allocateByteBuffer(int length) {
      return directBuffer
          ? ByteBuffer.allocateDirect(length)
          : ByteBuffer.allocate(length);
    }
  }

  @Benchmark
  public void pureJavaCrc32C(BenchmarkState state, Blackhole bh) {
    benchmark(state, bh,
        () -> newChecksumByteBufferFunction(PureJavaCrc32CByteBuffer::new));
  }

  @Benchmark
  public void newJava9Crc32C(BenchmarkState state, Blackhole bh) {
    if (!Shell.isJavaVersionAtLeast(9)) {
      System.exit(1);
    }
    benchmark(state, bh,
        () -> newChecksumByteBufferFunction(Java9Crc32CByteBuffer::create));
  }

  private void benchmark(BenchmarkState state, Blackhole bh,
      Supplier<Function<ByteBuffer, ByteString>> function) {
    for (int i = 0; i < state.count; i++) {
      ByteBuffer buffer = state.buffers.get(i);
      if (state.asReadOnly) {
        buffer = buffer.asReadOnlyBuffer();
      }
      ChunkBuffer chunkBuffer = ChunkBuffer.wrap(buffer);
      List<ByteString> checksumData = Checksum.computeChecksum(
          chunkBuffer, function.get(),
          state.kbPerChecksum << 10);
      bh.consume(checksumData);
    }
  }

}
