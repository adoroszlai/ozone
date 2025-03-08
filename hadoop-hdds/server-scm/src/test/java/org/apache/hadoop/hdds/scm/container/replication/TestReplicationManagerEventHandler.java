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


package org.apache.hadoop.hdds.scm.container.replication;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test the ReplicationManagerEventHandler class.
 */
public class TestReplicationManagerEventHandler {
  private ReplicationManager replicationManager;
  private ReplicationManagerEventHandler replicationManagerEventHandler;
  private EventPublisher publisher;

  @BeforeEach
  public void setUp() {
    replicationManager = mock(ReplicationManager.class);
    publisher = mock(EventPublisher.class);
    replicationManagerEventHandler = new ReplicationManagerEventHandler(replicationManager);
  }

  @Test
  public void testReplicationManagerEventHandler() {
    DatanodeDetails dataNodeDetails = MockDatanodeDetails.randomDatanodeDetails();
    replicationManagerEventHandler.onMessage(dataNodeDetails, publisher);

    verify(replicationManager).notifyNodeStateChange();
  }
}
