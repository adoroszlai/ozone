/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.shell.keys;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.shell.OzoneAddress;

import org.apache.commons.codec.digest.DigestUtils;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_DEFAULT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Puts a file into an ozone bucket.
 */
@Command(name = "put",
    description = "creates or overwrites an existing key")
public class PutKeyHandler extends KeyHandler {

  @Parameters(index = "1", arity = "1..1", description = "File to upload")
  private String fileName;

  @Option(names = {"-r", "--replication"},
      description =
          "Replication configuration of the new key. (this is replication "
              + "specific. for RATIS/STANDALONE you can use ONE or THREE) "
              + "Default is specified in the cluster-wide config.")
  private String replication;

  @Option(names = {"-t", "--type"},
      description = "Replication type of the new key. (use RATIS or " +
          "STAND_ALONE) Default is specified in the cluster-wide config.")
  private ReplicationType replicationType;

  @Override
  protected void execute(OzoneClient client, OzoneAddress address)
      throws IOException, OzoneClientException {

    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();

    File dataFile = new File(fileName);

    if (isVerbose()) {
      try (InputStream stream = new FileInputStream(dataFile)) {
        String hash = DigestUtils.md5Hex(stream);
        out().printf("File Hash : %s%n", hash);
      }
    }

    ReplicationConfig replicationConfig =
        ReplicationConfig.parse(replicationType, replication, getConf());

    OzoneVolume vol = client.getObjectStore().getVolume(volumeName);
    OzoneBucket bucket = vol.getBucket(bucketName);

    Map<String, String> keyMetadata = new HashMap<>();
    String gdprEnabled = bucket.getMetadata().get(OzoneConsts.GDPR_FLAG);
    if (Boolean.parseBoolean(gdprEnabled)) {
      keyMetadata.put(OzoneConsts.GDPR_FLAG, Boolean.TRUE.toString());
    }

    int chunkSize = (int) getConf().getStorageSize(OZONE_SCM_CHUNK_SIZE_KEY,
        OZONE_SCM_CHUNK_SIZE_DEFAULT, StorageUnit.BYTES);
    try (InputStream input = new FileInputStream(dataFile);
        OutputStream output = bucket.createKey(keyName, dataFile.length(),
            replicationConfig, keyMetadata)) {
      IOUtils.copyBytes(input, output, chunkSize);
    }
  }

}
