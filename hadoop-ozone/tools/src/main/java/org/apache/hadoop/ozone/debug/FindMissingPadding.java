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
package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.Shell;
import org.apache.hadoop.util.StringUtils;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Find EC keys possibly affected by missing padding blocks (HDDS-10681).
 */
@CommandLine.Command(name = "fmp",
    description = "List all potentially affected keys, optionally limited by volume/bucket/key URI.")
@MetaInfServices(SubcommandWithParent.class)
public class FindMissingPadding extends Handler implements SubcommandWithParent {

  @CommandLine.ParentCommand
  private OzoneDebug parent;

  @CommandLine.Parameters(arity = "0..1",
      description = Shell.OZONE_URI_DESCRIPTION)
  private String uri;

  @Override
  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress(uri);
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  protected void execute(OzoneClient ozoneClient, OzoneAddress address) throws IOException, OzoneClientException {
    ObjectStore objectStore = ozoneClient.getObjectStore();
    ClientProtocol rpcClient = objectStore.getClientProxy();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();
    if (!keyName.isEmpty()) {
      handleKey(rpcClient, volumeName, bucketName, keyName);
    } else if (!bucketName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      OzoneBucket bucket = volume.getBucket(bucketName);
      handleBucket(bucket, rpcClient);
    } else if (!volumeName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      handleVolume(volume, rpcClient);
    } else {
      for (Iterator<? extends OzoneVolume> it = objectStore.listVolumes(null); it.hasNext();) {
        handleVolume(it.next(), rpcClient);
      }
    }
  }

  private void handleVolume(OzoneVolume volume, ClientProtocol rpcClient) throws IOException {
    for (Iterator<? extends OzoneBucket> it = volume.listBuckets(null); it.hasNext();) {
      OzoneBucket bucket = it.next();
      handleBucket(bucket, rpcClient);
    }
  }

  private void handleBucket(OzoneBucket bucket, ClientProtocol rpcClient) throws IOException {
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    for (Iterator<? extends OzoneKey> it = bucket.listKeys(null); it.hasNext();) {
      OzoneKey key = it.next();
      if (isEC(key)) {
        handleKey(rpcClient, volumeName, bucketName, key.getName());
      } else {
        LOG.debug("Key {}/{}/{} is not EC", volumeName, bucketName, key.getName());
      }
    }
  }

  private void handleKey(ClientProtocol rpcClient, String volumeName, String bucketName, String keyName)
      throws IOException {
    OzoneKeyDetails keyDetails = rpcClient.getKeyDetails(volumeName, bucketName, keyName);
    if (isEC(keyDetails)) {
      handleECKey(keyDetails);
    }
  }

  private void handleECKey(OzoneKeyDetails keyDetails) {
    ECReplicationConfig ecConfig = (ECReplicationConfig) keyDetails.getReplicationConfig();
    List<OzoneKeyLocation> locations = keyDetails.getOzoneKeyLocations();
    if (!locations.isEmpty()) {
      OzoneKeyLocation lastLocation = locations.get(locations.size() - 1);
      long size = lastLocation.getLength();
      if (size <= (ecConfig.getData() - 1) * (long) ecConfig.getEcChunkSize()) {
        out().println(StringUtils.join("\t", Arrays.asList(
            keyDetails.getVolumeName() + "/" + keyDetails.getBucketName() + "/" + keyDetails.getName(),
            ecConfig.getData(),
            ecConfig.getEcChunkSize(),
            locations.size(),
            size
        )));
      } else {
        LOG.debug("Key {}/{}/{} has no padding",
            keyDetails.getVolumeName(), keyDetails.getBucketName(), keyDetails.getName());
      }
    } else {
      LOG.debug("Key {}/{}/{} has no locations",
          keyDetails.getVolumeName(), keyDetails.getBucketName(), keyDetails.getName());
    }
  }

  private boolean isEC(OzoneKey key) {
    return key.getReplicationConfig().getReplicationType() == HddsProtos.ReplicationType.EC;
  }
}
