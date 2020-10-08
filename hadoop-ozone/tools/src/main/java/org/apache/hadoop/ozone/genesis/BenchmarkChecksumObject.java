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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
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

/**
 * Benchmark for checksum implementations (using {@link Checksum} object.
 */
public class BenchmarkChecksumObject {

  /**
   * State (including parameters) for {@link BenchmarkChecksumObject}.
   */
  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param("10")
    private int count;

    @Param({"4194304"})
    private int dataLength;

    @Param({"1024"})
    private int kbPerChecksum;

    @Param({"false"})
    private boolean directBuffer;

    private List<ByteBuffer> buffers;
    private Checksum crc32c;

    @Setup
    public void createData() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      buffers = new ArrayList<>(count);
      crc32c = new Checksum(ContainerProtos.ChecksumType.CRC32C,
          kbPerChecksum << 10);
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
  public void checksumObjectNone(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    benchmark(state, bh,
        new Checksum(ContainerProtos.ChecksumType.NONE,
            state.kbPerChecksum << 10));
  }

  @Benchmark
  public void checksumObjectSha256(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    benchmark(state, bh,
        new Checksum(ContainerProtos.ChecksumType.SHA256,
            state.kbPerChecksum << 10));
  }

  @Benchmark
  public void checksumObjectCrc32C(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    benchmark(state, bh,
        new Checksum(ContainerProtos.ChecksumType.CRC32C,
            state.kbPerChecksum << 10));
  }

  @Benchmark
  public void preCreatedChecksumObjectCrc32C(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    benchmark(state, bh, state.crc32c);
  }

  private void benchmark(BenchmarkState state, Blackhole bh,
      Checksum checksum) throws OzoneChecksumException {
    for (int i = 0; i < state.count; i++) {
      ByteBuffer buffer = state.buffers.get(i);
      bh.consume(checksum.computeChecksum(buffer));
    }
  }
}
