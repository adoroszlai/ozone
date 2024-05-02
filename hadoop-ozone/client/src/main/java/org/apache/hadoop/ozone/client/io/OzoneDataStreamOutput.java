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

import org.apache.hadoop.hdds.scm.storage.ByteBufferStreamOutput;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * OzoneDataStreamOutput is used to write data into Ozone.
 */
public class OzoneDataStreamOutput extends ByteBufferOutputStream
    implements  KeyMetadataAware {

  private final ByteBufferStreamOutput byteBufferStreamOutput;
  private final OutputStream outputStream;

  /**
   * Constructs OzoneDataStreamOutput with KeyDataStreamOutput.
   *
   * @param byteBufferStreamOutput the underlying ByteBufferStreamOutput
   */
  public OzoneDataStreamOutput(ByteBufferStreamOutput byteBufferStreamOutput) {
    this.byteBufferStreamOutput = byteBufferStreamOutput;
    this.outputStream = byteBufferStreamOutput instanceof OutputStream ? (OutputStream) byteBufferStreamOutput : null;
  }

  @Override
  public void write(ByteBuffer b, int off, int len) throws IOException {
    byteBufferStreamOutput.write(b, off, len);
  }

  @Override
  public synchronized void flush() throws IOException {
    byteBufferStreamOutput.flush();
  }

  @Override
  public synchronized void close() throws IOException {
    //commitKey can be done here, if needed.
    byteBufferStreamOutput.close();
  }

  public OmMultipartCommitUploadPartInfo getCommitUploadPartInfo() {
    KeyDataStreamOutput keyDataStreamOutput = getKeyDataStreamOutput();
    if (keyDataStreamOutput != null) {
      return keyDataStreamOutput.getCommitUploadPartInfo();
    }
    // Otherwise return null.
    return null;
  }

  public OutputStream getOutputStream() {
    return outputStream;
  }

  private KeyDataStreamOutput getKeyDataStreamOutput() {
    return RpcClient.unwrap(getOutputStream(), KeyDataStreamOutput.class);
  }

  public ByteBufferStreamOutput getByteBufStreamOutput() {
    return byteBufferStreamOutput;
  }

  @Override
  public Map<String, String> getMetadata() {
    return ((KeyMetadataAware)this.byteBufferStreamOutput).getMetadata();
  }

}
