/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * OzoneOutputStream is used to write data into Ozone.
 */
public class OzoneOutputStream extends ByteArrayStreamOutput
    implements KeyMetadataAware {

  private final OutputStream outputStream;
  private final Syncable syncable;
  private final boolean enableHsync;

  /**
   * Constructs an instance with a {@link Syncable} {@link OutputStream}.
   *
   * @param outputStream an {@link OutputStream} which is {@link Syncable}.
   * @param enableHsync if false, hsync() executes flush() instead.
   */
  public OzoneOutputStream(Syncable outputStream, boolean enableHsync) {
    this(Optional.of(Objects.requireNonNull(outputStream,
                "outputStream == null"))
        .filter(s -> s instanceof OutputStream)
        .map(s -> (OutputStream)s)
        .orElseThrow(() -> new IllegalArgumentException(
            "The parameter syncable is not an OutputStream")),
        outputStream, enableHsync);
  }

  /**
   * Constructs an instance with a (non-{@link Syncable}) {@link OutputStream}
   * with an optional {@link Syncable} object.
   *
   * @param outputStream for writing data.
   * @param syncable an optional parameter
   *                 for accessing the {@link Syncable} feature.
   */
  public OzoneOutputStream(OutputStream outputStream, Syncable syncable) {
    this(outputStream, syncable, false);
  }

  /**
   * Constructs an instance with a (non-{@link Syncable}) {@link OutputStream}
   * with an optional {@link Syncable} object.
   *
   * @param outputStream for writing data.
   * @param syncable an optional parameter
   *                 for accessing the {@link Syncable} feature.
   * @param enableHsync if false, hsync() executes flush() instead.
   */
  public OzoneOutputStream(OutputStream outputStream, Syncable syncable,
      boolean enableHsync) {
    this.outputStream = Objects.requireNonNull(outputStream,
        "outputStream == null");
    this.syncable = syncable != null ? syncable
        : outputStream instanceof Syncable ? (Syncable) outputStream
        : null;
    this.enableHsync = enableHsync;
  }

  @Override
  public void write(int b) throws IOException {
    outputStream.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    outputStream.write(b, off, len);
  }

  @Override
  public synchronized void flush() throws IOException {
    outputStream.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    //commitKey can be done here, if needed.
    outputStream.close();
  }

  public void hsync() throws IOException {
    // Disable the feature flag restores the prior behavior.
    if (!enableHsync) {
      outputStream.flush();
      return;
    }
    if (syncable != null) {
      if (outputStream != syncable) {
        outputStream.flush();
      }
      syncable.hsync();
    } else {
      throw new UnsupportedOperationException(outputStream.getClass()
          + " is not " + Syncable.class.getSimpleName());
    }
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    KeyOutputStream keyOutputStream = RpcClient.unwrap(getOutputStream(), KeyOutputStream.class);
    return keyOutputStream != null ? keyOutputStream.getCommitUploadPartInfo() : null;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  @Override
  public Map<String, String> getMetadata() {
    KeyMetadataAware metadataAware = RpcClient.unwrap(getOutputStream(), KeyMetadataAware.class);
    return metadataAware != null ? metadataAware.getMetadata() : null;
  }
}
