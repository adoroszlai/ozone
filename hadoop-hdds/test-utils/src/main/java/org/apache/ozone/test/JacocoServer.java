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
package org.apache.ozone.test;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.jacoco.core.data.ExecutionDataWriter;
import org.jacoco.core.data.IExecutionDataVisitor;
import org.jacoco.core.data.ISessionInfoVisitor;
import org.jacoco.core.runtime.RemoteControlReader;
import org.jacoco.core.runtime.RemoteControlWriter;

/**
 * Simple TPC server to collect all the Jacoco coverage data.
 */
public final class JacocoServer {

  private static final Object LOCK_MONITOR = new Object();
  private static final int PORT = 6300;
  private static final String DESTINATION_FILE = "/tmp/jacoco-combined.exec";

  private JacocoServer() {
  }

  @SuppressWarnings("checkstyle:EmptyStatement")
  public static void main(String[] args) throws IOException {
    final BufferedOutputStream output = new BufferedOutputStream(Files.newOutputStream(Paths.get(DESTINATION_FILE)));
    ExecutionDataWriter destination = new ExecutionDataWriter(output);
    ServerSocket serverSocket = new ServerSocket(PORT);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        destination.flush();
      } catch (Exception ex) {
        ex.printStackTrace();
      }
      IOUtils.closeQuietly(output, serverSocket);
    }));

    while (true) {
      final Socket socket = serverSocket.accept();
      new Thread(() -> {
        try {
          RemoteControlWriter writer =
              new RemoteControlWriter(socket.getOutputStream());
          RemoteControlReader reader =
              new RemoteControlReader(socket.getInputStream());
          reader.setSessionInfoVisitor(
              synchronizedCall(destination::visitSessionInfo));
          reader.setExecutionDataVisitor(
              synchronizedCall(destination::visitClassExecution));
          while (reader.read()) {
            ;//read until the end of the stream.
          }
          synchronized (LOCK_MONITOR) {
            destination.flush();
          }
        } catch (Exception ex) {
          ex.printStackTrace();
        } finally {
          IOUtils.closeQuietly(socket);
        }
      }).start();
    }

  }

  /**
   * Make the ISessionInfoVisitor call synchronized.
   */
  public static ISessionInfoVisitor synchronizedCall(
      ISessionInfoVisitor origin) {
    return data -> {
      synchronized (LOCK_MONITOR) {
        origin.visitSessionInfo(data);
      }
    };
  }

  /**
   * Make the IExecutionDataVisitor call synchronized.
   */
  public static IExecutionDataVisitor synchronizedCall(
      IExecutionDataVisitor origin) {
    return data -> {
      synchronized (LOCK_MONITOR) {
        origin.visitClassExecution(data);
      }
    };
  }
}
