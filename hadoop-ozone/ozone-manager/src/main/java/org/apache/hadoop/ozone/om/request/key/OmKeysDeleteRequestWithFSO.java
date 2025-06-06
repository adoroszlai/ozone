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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.OzoneConsts.DELETED_HSYNC_KEY;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.PARTIAL_DELETE;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.om.response.key.OMKeysDeleteResponseWithFSO;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles DeleteKeys request for recursive bucket deletion.
 */
public class OmKeysDeleteRequestWithFSO extends OMKeysDeleteRequest {

  private static final Logger LOG = LoggerFactory.getLogger(OmKeysDeleteRequestWithFSO.class);

  public OmKeysDeleteRequestWithFSO(
      OzoneManagerProtocolProtos.OMRequest omRequest,
      BucketLayout bucketLayout) {
    super(omRequest, bucketLayout);
  }

  @Override
  protected OmKeyInfo getOmKeyInfo(
      OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName)
      throws IOException {
    OzoneFileStatus keyStatus = getOzoneKeyStatus(
        ozoneManager, omMetadataManager, volumeName, bucketName, keyName);
    return keyStatus != null ? keyStatus.getKeyInfo() : null;
  }

  @Override
  protected void addKeyToAppropriateList(List<OmKeyInfo> omKeyInfoList,
      OmKeyInfo omKeyInfo, List<OmKeyInfo> dirList, OzoneFileStatus keyStatus) {
    if (keyStatus.isDirectory()) {
      dirList.add(omKeyInfo);
    } else {
      omKeyInfoList.add(omKeyInfo);
    }
  }

  @Override
  protected OzoneFileStatus getOzoneKeyStatus(
      OzoneManager ozoneManager, OMMetadataManager omMetadataManager,
      String volumeName, String bucketName, String keyName) throws IOException {
    return OMFileRequest.getOMKeyInfoIfExists(omMetadataManager,
        volumeName, bucketName, keyName, 0,
        ozoneManager.getDefaultReplicationConfig());
  }

  @Override
  protected long markKeysAsDeletedInCache(
          OzoneManager ozoneManager, long trxnLogIndex,
          List<OmKeyInfo> omKeyInfoList,
          List<OmKeyInfo> dirList, OMMetadataManager omMetadataManager,
          long quotaReleased, Map<String, OmKeyInfo> openKeyInfoMap) throws IOException {

    // Mark all keys which can be deleted, in cache as deleted.
    for (OmKeyInfo omKeyInfo : omKeyInfoList) {
      final long volumeId = omMetadataManager.getVolumeId(
              omKeyInfo.getVolumeName());
      final long bucketId = omMetadataManager.getBucketId(
              omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
      final long parentId = omKeyInfo.getParentObjectID();
      final String fileName = omKeyInfo.getFileName();
      omMetadataManager.getKeyTable(getBucketLayout()).addCacheEntry(
          new CacheKey<>(omMetadataManager
              .getOzonePathKey(volumeId, bucketId, parentId, fileName)),
          CacheValue.get(trxnLogIndex));

      omKeyInfo.setUpdateID(trxnLogIndex);
      quotaReleased += sumBlockLengths(omKeyInfo);

      // If omKeyInfo has hsync metadata, delete its corresponding open key as well
      String hsyncClientId = omKeyInfo.getMetadata().get(OzoneConsts.HSYNC_CLIENT_ID);
      if (hsyncClientId != null) {
        Table<String, OmKeyInfo> openKeyTable = omMetadataManager.getOpenKeyTable(getBucketLayout());
        String dbOpenKey = omMetadataManager.getOpenFileName(volumeId, bucketId, parentId, fileName, hsyncClientId);
        OmKeyInfo openKeyInfo = openKeyTable.get(dbOpenKey);
        if (openKeyInfo != null) {
          openKeyInfo.getMetadata().put(DELETED_HSYNC_KEY, "true");
          openKeyTable.addCacheEntry(dbOpenKey, openKeyInfo, trxnLogIndex);
          // Add to the map of open keys to be deleted.
          openKeyInfoMap.put(dbOpenKey, openKeyInfo);
        } else {
          LOG.warn("Potentially inconsistent DB state: open key not found with dbOpenKey '{}'", dbOpenKey);
        }
      }
    }

    // Mark directory keys.
    for (OmKeyInfo omKeyInfo : dirList) {
      final long volumeId = omMetadataManager.getVolumeId(
              omKeyInfo.getVolumeName());
      final long bucketId = omMetadataManager.getBucketId(
              omKeyInfo.getVolumeName(), omKeyInfo.getBucketName());
      omMetadataManager.getDirectoryTable().addCacheEntry(new CacheKey<>(
              omMetadataManager.getOzonePathKey(volumeId, bucketId,
                      omKeyInfo.getParentObjectID(),
                  omKeyInfo.getFileName())),
          CacheValue.get(trxnLogIndex));

      omKeyInfo.setUpdateID(trxnLogIndex);
      quotaReleased += sumBlockLengths(omKeyInfo);
    }
    return quotaReleased;
  }

  @Nonnull @Override
  protected OMClientResponse getOmClientResponse(OzoneManager ozoneManager,
      List<OmKeyInfo> omKeyInfoList, List<OmKeyInfo> dirList,
      OzoneManagerProtocolProtos.OMResponse.Builder omResponse,
      OzoneManagerProtocolProtos.DeleteKeyArgs.Builder unDeletedKeys,
      Map<String, ErrorInfo> keyToErrors,
      boolean deleteStatus, OmBucketInfo omBucketInfo, long volumeId, Map<String, OmKeyInfo> openKeyInfoMap) {
    OMClientResponse omClientResponse;
    List<OzoneManagerProtocolProtos.DeleteKeyError> deleteKeyErrors = new ArrayList<>();
    for (Map.Entry<String, ErrorInfo>  key : keyToErrors.entrySet()) {
      deleteKeyErrors.add(OzoneManagerProtocolProtos.DeleteKeyError.newBuilder()
          .setKey(key.getKey()).setErrorCode(key.getValue().getCode())
          .setErrorMsg(key.getValue().getMessage()).build());
    }
    omClientResponse = new OMKeysDeleteResponseWithFSO(omResponse
        .setDeleteKeysResponse(
            OzoneManagerProtocolProtos.DeleteKeysResponse.newBuilder()
                .setStatus(deleteStatus).setUnDeletedKeys(unDeletedKeys).addAllErrors(deleteKeyErrors))
        .setStatus(deleteStatus ? OK : PARTIAL_DELETE).setSuccess(deleteStatus)
        .build(), omKeyInfoList, dirList,
        omBucketInfo.copyObject(), volumeId, openKeyInfoMap);
    return omClientResponse;
  }
}
