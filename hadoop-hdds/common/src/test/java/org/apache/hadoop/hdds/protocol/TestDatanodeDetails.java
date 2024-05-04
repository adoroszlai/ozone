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
package org.apache.hadoop.hdds.protocol;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.ALL_PORTS;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.V0_PORTS;
import static org.apache.hadoop.ozone.ClientVersion.DEFAULT_VERSION;
import static org.apache.hadoop.ozone.ClientVersion.VERSION_HANDLES_UNKNOWN_DN_PORTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for {@link DatanodeDetails}.
 */
public class TestDatanodeDetails {

  @Test
  void protoIncludesNewPortsOnlyForV1() {
    DatanodeDetails subject = MockDatanodeDetails.randomDatanodeDetails();

    HddsProtos.DatanodeDetailsProto proto =
        subject.toProto(DEFAULT_VERSION.toProtoValue());
    assertPorts(proto, V0_PORTS);

    HddsProtos.DatanodeDetailsProto protoV1 =
        subject.toProto(VERSION_HANDLES_UNKNOWN_DN_PORTS.toProtoValue());
    assertPorts(protoV1, ALL_PORTS);
  }
  @Test
  void testRequiredPortsProto() {
    DatanodeDetails subject = MockDatanodeDetails.randomDatanodeDetails();
    List<Port.Name> requiredPorts = Arrays.asList(Port.Name.STANDALONE, Name.RATIS);
    HddsProtos.DatanodeDetailsProto proto =
        subject.toProto(subject.getCurrentVersion(), requiredPorts);
    assertPorts(proto, ImmutableSet.copyOf(requiredPorts));
  }

  public static void assertPorts(HddsProtos.DatanodeDetailsProto dn,
      Set<Port.Name> expectedPorts) throws IllegalArgumentException {
    assertEquals(expectedPorts.size(), dn.getPortsCount());
    for (HddsProtos.Port port : dn.getPortsList()) {
      assertThat(expectedPorts).contains(Port.Name.valueOf(port.getName()));
    }
  }

}
