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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType.EC;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.BUCKET_NOT_FOUND;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.KEY_NOT_FOUND;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_COPY_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_DIRECTIVE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapInQuotes;

import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.time.Instant;
import java.util.Map;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.CopyDirective;
import org.apache.hadoop.util.Time;

class CopyKeyHandler extends ObjectOperationHandler {

  @Override
  Response handlePutRequest(ObjectRequestContext context, String keyName, InputStream body)
      throws IOException, OS3Exception {
    if (context.ignore(getAction())) {
      return null;
    }

    try {
      //Copy object, as copy source available.
      CopyObjectResponse copyObjectResponse = copyObject(context,
          context.getBucket().getName(), keyName, getReplicationConfig(context.getBucket()));
      return Response.status(Response.Status.OK)
          .entity(copyObjectResponse)
          .header("Connection", "close")
          .build();
    } catch (Exception e) {
      getMetrics().updateCopyObjectFailureStats(context.getStartNanos());
      throw e;
    }
  }

  @SuppressWarnings("checkstyle:ParameterNumber")
  void copy(ObjectRequestContext context, DigestInputStream src, long srcKeyLen,
      String destKey, String destBucket,
      ReplicationConfig replication,
      Map<String, String> metadata,
      Map<String, String> tags)
      throws IOException {
    OzoneVolume volume = context.getVolume();
    PerformanceStringBuilder perf = context.getPerf();
    long copyLength;
    if (isDatastreamEnabled() && !(replication != null &&
        replication.getReplicationType() == EC) &&
        srcKeyLen > getDatastreamMinLength()) {
      perf.appendStreamMode();
      copyLength = ObjectEndpointStreaming
          .copyKeyWithStream(volume.getBucket(destBucket), destKey, srcKeyLen,
              getChunkSize(), replication, metadata, src, perf, context.getStartNanos(), tags);
    } else {
      try (OzoneOutputStream dest = getClientProtocol()
          .createKey(volume.getName(), destBucket, destKey, srcKeyLen,
              replication, metadata, tags)) {
        long metadataLatencyNs =
            getMetrics().updateCopyKeyMetadataStats(context.getStartNanos());
        perf.appendMetaLatencyNanos(metadataLatencyNs);
        copyLength = IOUtils.copyLarge(src, dest, 0, srcKeyLen, new byte[getIOBufferSize(srcKeyLen)]);
        String md5Hash = DatatypeConverter.printHexBinary(src.getMessageDigest().digest()).toLowerCase();
        dest.getMetadata().put(OzoneConsts.ETAG, md5Hash);
      }
    }
    getMetrics().incCopyObjectSuccessLength(copyLength);
    perf.appendSizeBytes(copyLength);
  }

  private CopyObjectResponse copyObject(ObjectRequestContext context,
      String destBucket, String destkey, ReplicationConfig replicationConfig
  ) throws OS3Exception, IOException {
    OzoneVolume volume = context.getVolume();
    String copyHeader = getHeaders().getHeaderString(COPY_SOURCE_HEADER);
    String storageType = getHeaders().getHeaderString(STORAGE_CLASS_HEADER);
    boolean storageTypeDefault = StringUtils.isEmpty(storageType);

    Pair<String, String> result = parseSourceHeader(copyHeader);

    String sourceBucket = result.getLeft();
    String sourceKey = result.getRight();
    DigestInputStream sourceDigestInputStream = null;

    if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
      String sourceBucketOwner = context.getVolume().getBucket(sourceBucket).getOwner();
      // The destBucket owner has already been checked in the caller method
      S3Owner.verifyBucketOwnerConditionOnCopyOperation(getHeaders(), sourceBucket, sourceBucketOwner, null, null);
    }
    try {
      OzoneKeyDetails sourceKeyDetails = getClientProtocol().getKeyDetails(
          context.getVolume().getName(), sourceBucket, sourceKey);
      // Checking whether we trying to copying to it self.
      if (sourceBucket.equals(destBucket) && sourceKey
          .equals(destkey)) {
        // When copying to same storage type when storage type is provided,
        // we should not throw exception, as aws cli checks if any of the
        // options like storage type are provided or not when source and
        // dest are given same
        if (storageTypeDefault) {
          OS3Exception ex = newError(S3ErrorTable.INVALID_REQUEST, copyHeader);
          ex.setErrorMessage("This copy request is illegal because it is " +
              "trying to copy an object to it self itself without changing " +
              "the object's metadata, storage class, website redirect " +
              "location or encryption attributes.");
          throw ex;
        } else {
          // TODO: Actually here we should change storage type, as ozone
          // still does not support this just returning dummy response
          // for now
          CopyObjectResponse copyObjectResponse = new CopyObjectResponse();
          copyObjectResponse.setETag(wrapInQuotes(sourceKeyDetails.getMetadata().get(OzoneConsts.ETAG)));
          copyObjectResponse.setLastModified(Instant.ofEpochMilli(
              Time.now()));
          return copyObjectResponse;
        }
      }
      long sourceKeyLen = sourceKeyDetails.getDataSize();

      // Object tagging in copyObject with tagging directive
      Map<String, String> tags;
      String tagCopyDirective = getHeaders().getHeaderString(TAG_DIRECTIVE_HEADER);
      if (StringUtils.isEmpty(tagCopyDirective) || tagCopyDirective.equals(CopyDirective.COPY.name())) {
        // Tag-set will be copied from the source directly
        tags = sourceKeyDetails.getTags();
      } else if (tagCopyDirective.equals(CopyDirective.REPLACE.name())) {
        // Replace the tags with the tags from the request headers
        tags = getTaggingFromHeaders(getHeaders());
      } else {
        OS3Exception ex = newError(INVALID_ARGUMENT, tagCopyDirective);
        ex.setErrorMessage("An error occurred (InvalidArgument) " +
            "when calling the CopyObject operation: " +
            "The tagging copy directive specified is invalid. Valid values are COPY or REPLACE.");
        throw ex;
      }

      // Custom metadata in copyObject with metadata directive
      Map<String, String> customMetadata;
      String metadataCopyDirective = getHeaders().getHeaderString(CUSTOM_METADATA_COPY_DIRECTIVE_HEADER);
      if (StringUtils.isEmpty(metadataCopyDirective) || metadataCopyDirective.equals(CopyDirective.COPY.name())) {
        // The custom metadata will be copied from the source key
        customMetadata = sourceKeyDetails.getMetadata();
      } else if (metadataCopyDirective.equals(CopyDirective.REPLACE.name())) {
        // Replace the metadata with the metadata form the request headers
        customMetadata = getCustomMetadataFromHeaders(getHeaders().getRequestHeaders());
      } else {
        OS3Exception ex = newError(INVALID_ARGUMENT, metadataCopyDirective);
        ex.setErrorMessage("An error occurred (InvalidArgument) " +
            "when calling the CopyObject operation: " +
            "The metadata copy directive specified is invalid. Valid values are COPY or REPLACE.");
        throw ex;
      }

      try (OzoneInputStream src = getClientProtocol().getKey(volume.getName(), sourceBucket, sourceKey)) {
        getMetrics().updateCopyKeyMetadataStats(context.getStartNanos());
        sourceDigestInputStream = new DigestInputStream(src, getMD5DigestInstance());
        copy(context, sourceDigestInputStream, sourceKeyLen, destkey, destBucket, replicationConfig,
            customMetadata, tags);
      }

      final OzoneKeyDetails destKeyDetails = getClientProtocol().getKeyDetails(volume.getName(), destBucket, destkey);

      getMetrics().updateCopyObjectSuccessStats(context.getStartNanos());
      CopyObjectResponse copyObjectResponse = new CopyObjectResponse();
      copyObjectResponse.setETag(wrapInQuotes(destKeyDetails.getMetadata().get(OzoneConsts.ETAG)));
      copyObjectResponse.setLastModified(destKeyDetails.getModificationTime());
      return copyObjectResponse;
    } catch (OMException ex) {
      if (ex.getResult() == KEY_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_KEY, sourceKey, ex);
      } else if (ex.getResult() == BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, sourceBucket, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            destBucket + "/" + destkey, ex);
      }
      throw ex;
    } finally {
      // Reset the thread-local message digest instance in case of exception
      // and MessageDigest#digest is never called
      if (sourceDigestInputStream != null) {
        sourceDigestInputStream.getMessageDigest().reset();
      }
    }
  }

  private S3GAction getAction() {
    return getHeaders().getHeaderString(COPY_SOURCE_HEADER) != null
        && HttpMethod.PUT.equals(getContext().getMethod())
        ? S3GAction.COPY_OBJECT
        : null;
  }
}
