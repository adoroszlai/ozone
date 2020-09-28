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

import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.ozone.common.Checksum;
import org.apache.hadoop.ozone.common.ChecksumData;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.common.Java9Crc32CByteBuffer;
import org.apache.hadoop.ozone.common.OzoneChecksumException;
import org.apache.hadoop.ozone.common.PureJavaCrc32ByteBuffer;
import org.apache.hadoop.ozone.common.PureJavaCrc32CByteBuffer;

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
 * Benchmark for checksum implementations.
 */
public class BenchmarkChecksum {

  private static final Checksum.Algorithm CRC32C =
      Checksum.Algorithm.valueOf("CRC32C");

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param("10")
    private int count;

    //@Param({"4", "16"})
    @Param({"4"})
    private int dataLengthMB;

    //@Param({"256", "1024"})
    @Param({"1024"})
    private int kbPerChecksum;

    //@Param({"true", "false"})
    @Param({"false"})
    private boolean directBuffer;

    private List<ByteBuffer> buffers;
    private List<Checksum> crc32cs;

    @Setup
    public void createData() {
      ThreadLocalRandom random = ThreadLocalRandom.current();
      buffers = new ArrayList<>(count);
      crc32cs = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        buffers.add(newData(random));
        crc32cs.add(new Checksum(ChecksumType.CRC32C, kbPerChecksum << 10));
      }
    }

    private ByteBuffer newData(Random random) {
      final byte[] bytes = new byte[dataLengthMB << 20];
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
  public void pureJavaCrc32(BenchmarkState state, Blackhole bh) {
    benchmark(state, bh,
        () -> newChecksumByteBufferFunction(PureJavaCrc32ByteBuffer::new));
  }

  @Benchmark
  public void pureJavaCrc32C(BenchmarkState state, Blackhole bh) {
    benchmark(state, bh,
        () -> newChecksumByteBufferFunction(PureJavaCrc32CByteBuffer::new));
  }

  @Benchmark
  public void newJava9Crc32C(BenchmarkState state, Blackhole bh) {
    benchmark(state, bh,
        () -> newChecksumByteBufferFunction(Java9Crc32CByteBuffer::create));
  }

  @Benchmark
  public void checksumObjectNone(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    benchmark(state, bh,
        new Checksum(ChecksumType.NONE, state.kbPerChecksum << 10));
  }

  @Benchmark
  public void checksumObjectSha256(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    benchmark(state, bh,
        new Checksum(ChecksumType.SHA256, state.kbPerChecksum << 10));
  }

  @Benchmark
  public void checksumObjectCrc32C(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    benchmark(state, bh,
        new Checksum(ChecksumType.CRC32C, state.kbPerChecksum << 10));
  }

  @Benchmark
  public void preCreatedChecksumObjectCrc32C(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    for (int i = 0; i < state.count; i++) {
      ByteBuffer buffer = state.buffers.get(i);
      ChecksumData checksumData = state.crc32cs.get(i).computeChecksum(buffer);
      bh.consume(checksumData);
    }
  }

  @Benchmark
  public void preCreatedSkipReadonly(BenchmarkState state, Blackhole bh)
      throws OzoneChecksumException {
    for (int i = 0; i < state.count; i++) {
      ByteBuffer buffer = state.buffers.get(i);
      Checksum checksum = state.crc32cs.get(i);
      bh.consume(checksum.computeChecksum(ChunkBuffer.wrap(buffer)));
    }
  }

  @Benchmark
  public void convertToReadOnlyByteBuffer(BenchmarkState state, Blackhole bh) {
    for (int i = 0; i < state.count; i++) {
      bh.consume(state.buffers.get(i).asReadOnlyBuffer());
    }
  }

  @Benchmark
  public void toAlgo(BenchmarkState state, Blackhole bh) {
    for (int i = 0; i < state.count; i++) {
      bh.consume(Checksum.Algorithm.valueOf(ChecksumType.CRC32C.name()));
    }
  }

  @Benchmark
  public void newChecksumFunction(BenchmarkState state, Blackhole bh) {
    for (int i = 0; i < state.count; i++) {
      bh.consume(CRC32C.newChecksumFunction());
    }
  }

  private void benchmark(BenchmarkState state, Blackhole bh,
      Checksum checksum) throws OzoneChecksumException {
    for (int i = 0; i < state.count; i++) {
      ByteBuffer buffer = state.buffers.get(i);
      bh.consume(checksum.computeChecksum(buffer));
    }
  }

  private void benchmark(BenchmarkState state, Blackhole bh,
      Supplier<Function<ByteBuffer, ByteString>> function) {
    for (int i = 0; i < state.count; i++) {
      List<ByteString> checksumData = Checksum.computeChecksum(
          ChunkBuffer.wrap(state.buffers.get(i)),
          function.get(),
          state.kbPerChecksum << 10);
      bh.consume(checksumData);
    }
  }

}
