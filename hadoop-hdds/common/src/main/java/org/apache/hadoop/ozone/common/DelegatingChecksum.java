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

import java.util.zip.Checksum;

/**
 * Base class for implementation that delegates to another {@link Checksum}.
 */
public class DelegatingChecksum implements Checksum {

  private final Checksum delegate;

  public DelegatingChecksum(Checksum delegate) {
    this.delegate = delegate;
  }

  @Override
  public void update(int b) {
    delegate.update(b);
  }

  @Override
  public void update(byte[] b, int off, int len) {
    delegate.update(b, off, len);
  }

  @Override
  public long getValue() {
    return delegate.getValue();
  }

  @Override
  public void reset() {
    delegate.reset();
  }
}
