/**
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

package org.apache.hadoop.ozone.recon.spi.impl;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.ozone.recon.ReconUtils;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Class to test StorageContainerServiceProviderImpl APIs.
 */
public class TestStorageContainerServiceProviderImpl {

  private Injector injector;
  private HddsProtos.PipelineID pipelineID;

  @Before
  public void setup() {
    injector = Guice.createInjector(new AbstractModule() {
      @Override
      protected void configure() {
        try {
          StorageContainerLocationProtocol mockScmClient = mock(
              StorageContainerLocationProtocol.class);
          ReconUtils reconUtils =  new ReconUtils();
          File testDir = GenericTestUtils.getRandomizedTestDir();
          OzoneConfiguration conf = new OzoneConfiguration();
          conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getPath());
          pipelineID = PipelineID.randomId().getProtobuf();
          when(mockScmClient.getPipeline(pipelineID))
              .thenReturn(mock(Pipeline.class));
          bind(StorageContainerLocationProtocol.class)
              .toInstance(mockScmClient);
          bind(StorageContainerServiceProvider.class)
              .to(StorageContainerServiceProviderImpl.class);
          bind(OzoneConfiguration.class).
              toInstance(conf);
          bind(ReconUtils.class).toInstance(reconUtils);
        } catch (Exception e) {
          Assert.fail();
        }
      }
    });
  }

  @Test
  public void testGetPipelines() throws IOException {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationProtocol scmClient =
        injector.getInstance(StorageContainerLocationProtocol.class);
    scmProvider.getPipelines();
    verify(scmClient, times(1)).listPipelines();
  }

  @Test
  public void testGetPipeline() throws IOException {
    StorageContainerServiceProvider scmProvider =
        injector.getInstance(StorageContainerServiceProvider.class);
    StorageContainerLocationProtocol scmClient =
        injector.getInstance(StorageContainerLocationProtocol.class);
    Pipeline pipeline = scmProvider.getPipeline(pipelineID);
    assertNotNull(pipeline);
    verify(scmClient, times(1))
        .getPipeline(pipelineID);
  }
}