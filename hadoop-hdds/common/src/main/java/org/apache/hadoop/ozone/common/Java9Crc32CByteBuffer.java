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
package org.apache.hadoop.ozone.common;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;

import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ChecksumByteBuffer} implementation based on Java 9's new CRC32C class.
 */
public class Java9Crc32CByteBuffer extends DelegatingChecksum
    implements ChecksumByteBuffer {

  private static final Logger LOG =
    LoggerFactory.getLogger(Java9Crc32CByteBuffer.class);

  private static volatile boolean useJava9Crc32C =
    Shell.isJavaVersionAtLeast(9);

  /**
   * Creates a {@link Java9Crc32CByteBuffer} if running on Java 9 or later,
   * otherwise a {@link PureJavaCrc32CByteBuffer}.
   */
  public static ChecksumByteBuffer create() {
    if (useJava9Crc32C) {
      try {
        return new Java9Crc32CByteBuffer(Factory.createChecksum());
      } catch (ExceptionInInitializerError | RuntimeException e) {
        // will return java-based implementation
        useJava9Crc32C = false;
        LOG.error("CRC32C creation failed, switching to PureJavaCrc32C", e);
      }
    }
    return new PureJavaCrc32CByteBuffer();
  }

  private Java9Crc32CByteBuffer(Checksum delegate) {
    super(delegate);
  }

  @Override
  public void update(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      int pos = buffer.position();
      int len = buffer.remaining();
      if (len > 0) {
        update(buffer.array(), pos, len);
        buffer.position(pos + len);
      }
    } else {
      byte[] arr = new byte[buffer.remaining()];
      buffer.get(arr);
      update(arr, 0, arr.length);
    }
  }

  /**
   * Holds constructor handle to let it be initialized on demand.
   */
  private static class Factory {
    private static final MethodHandle NEW_CRC32C_MH;

    static {
      MethodHandle newCRC32C;
      try {
        newCRC32C = MethodHandles.publicLookup()
            .findConstructor(
                Class.forName("java.util.zip.CRC32C"),
                MethodType.methodType(void.class)
            );
      } catch (ReflectiveOperationException e) {
        // Should not reach here.
        throw new IllegalStateException(e);
      }
      NEW_CRC32C_MH = newCRC32C;
    }

    static java.util.zip.Checksum createChecksum() {
      try {
        // Should throw nothing
        return (Checksum) NEW_CRC32C_MH.invoke();
      } catch (RuntimeException e) {
        throw e;
      } catch (Throwable t) {
        throw new IllegalStateException(t);
      }
    }
  }
}
