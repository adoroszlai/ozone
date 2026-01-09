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
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_UPLOAD;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.PRECOND_FAILED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER_RANGE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_MODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_IF_UNMODIFIED_SINCE;
import static org.apache.hadoop.ozone.s3.util.S3Consts.DECODED_CONTENT_LENGTH_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.stripQuotes;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapInQuotes;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.s3.MultiDigestInputStream;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.RangeHeader;
import org.apache.hadoop.ozone.s3.util.RangeHeaderParserUtil;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3Utils;
import org.apache.ratis.util.function.CheckedRunnable;

class MultipartKeyHandler extends ObjectOperationHandler {

  private S3GAction getAction() {
    if (queryParams().get(S3Consts.QueryParams.UPLOAD_ID) == null) {
      return null;
    }

    switch (getContext().getMethod()) {
    case HttpMethod.DELETE:
      return S3GAction.ABORT_MULTIPART_UPLOAD;
    case HttpMethod.GET:
      return S3GAction.LIST_PARTS;
    case HttpMethod.PUT:
      return getHeaders().getHeaderString(COPY_SOURCE_HEADER) == null
          ? S3GAction.CREATE_MULTIPART_KEY
          : S3GAction.CREATE_MULTIPART_KEY_BY_COPY;
    default:
      return null;
    }
  }

  @Override
  Response handlePutRequest(ObjectRequestContext context, String keyName, InputStream body) throws IOException {
    if (context.ignore(getAction())) {
      return null;
    }

    final long length = headerParams().getLong(HttpHeaders.CONTENT_LENGTH, 0);
    return createMultipartKey(context, keyName, length, body);
  }

  @SuppressWarnings("checkstyle:MethodLength")
  private Response createMultipartKey(ObjectRequestContext context,
      String key, long length,
      final InputStream body
  ) throws IOException, OS3Exception {
    long startNanos = context.getStartNanos();
    final String uploadID = queryParams().get(S3Consts.QueryParams.UPLOAD_ID);
    final int partNumber = queryParams().getInt(S3Consts.QueryParams.PART_NUMBER, 0);
    String copyHeader = null;
    MultiDigestInputStream multiDigestInputStream = null;
    PerformanceStringBuilder perf = context.getPerf();
    final String bucketName = context.getBucketName();
    try {
      OzoneVolume volume = context.getVolume();
      OzoneBucket ozoneBucket = context.getBucket();
      String amzDecodedLength = getHeaders().getHeaderString(DECODED_CONTENT_LENGTH_HEADER);
      S3ChunkInputStreamInfo chunkInputStreamInfo = getS3ChunkInputStreamInfo(
          body, length, amzDecodedLength, key);
      multiDigestInputStream = chunkInputStreamInfo.getMultiDigestInputStream();
      length = chunkInputStreamInfo.getEffectiveLength();

      copyHeader = getHeaders().getHeaderString(COPY_SOURCE_HEADER);
      ReplicationConfig replicationConfig = getReplicationConfig(ozoneBucket);

      boolean enableEC = (replicationConfig != null && replicationConfig.getReplicationType() == EC) ||
          ozoneBucket.getReplicationConfig() instanceof ECReplicationConfig;

      if (isDatastreamEnabled() && !enableEC && copyHeader == null) {
        perf.appendStreamMode();
        return ObjectEndpointStreaming
            .createMultipartKey(ozoneBucket, key, length, partNumber,
                uploadID, getChunkSize(), multiDigestInputStream, perf, getHeaders());
      }
      // OmMultipartCommitUploadPartInfo can only be gotten after the
      // OzoneOutputStream is closed, so we need to save the OzoneOutputStream
      final OzoneOutputStream outputStream;
      long metadataLatencyNs;
      if (copyHeader != null) {
        Pair<String, String> result = parseSourceHeader(copyHeader);
        String sourceBucket = result.getLeft();
        String sourceKey = result.getRight();
        if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
          String sourceBucketOwner = volume.getBucket(sourceBucket).getOwner();
          S3Owner.verifyBucketOwnerConditionOnCopyOperation(getHeaders(), sourceBucket, sourceBucketOwner, bucketName,
              ozoneBucket.getOwner());
        }

        OzoneKeyDetails sourceKeyDetails = getClientProtocol().getKeyDetails(
            volume.getName(), sourceBucket, sourceKey);
        String range =
            getHeaders().getHeaderString(COPY_SOURCE_HEADER_RANGE);
        RangeHeader rangeHeader = null;
        if (range != null) {
          rangeHeader = RangeHeaderParserUtil.parseRangeHeader(range, 0);
          // When copy Range, the size of the target key is the
          // length specified by COPY_SOURCE_HEADER_RANGE.
          length = rangeHeader.getEndOffset() -
              rangeHeader.getStartOffset() + 1;
        } else {
          length = sourceKeyDetails.getDataSize();
        }
        Long sourceKeyModificationTime = sourceKeyDetails
            .getModificationTime().toEpochMilli();
        String copySourceIfModifiedSince =
            getHeaders().getHeaderString(COPY_SOURCE_IF_MODIFIED_SINCE);
        String copySourceIfUnmodifiedSince =
            getHeaders().getHeaderString(COPY_SOURCE_IF_UNMODIFIED_SINCE);
        if (!checkCopySourceModificationTime(sourceKeyModificationTime,
            copySourceIfModifiedSince, copySourceIfUnmodifiedSince)) {
          throw newError(PRECOND_FAILED, sourceBucket + "/" + sourceKey);
        }

        try (OzoneInputStream sourceObject = sourceKeyDetails.getContent()) {
          long copyLength;
          if (range != null) {
            final long skipped =
                sourceObject.skip(rangeHeader.getStartOffset());
            if (skipped != rangeHeader.getStartOffset()) {
              throw new EOFException(
                  "Bytes to skip: "
                      + rangeHeader.getStartOffset() + " actual: " + skipped);
            }
          }
          try (OzoneOutputStream ozoneOutputStream = getClientProtocol()
              .createMultipartKey(volume.getName(), bucketName, key, length,
                  partNumber, uploadID)) {
            metadataLatencyNs =
                getMetrics().updateCopyKeyMetadataStats(startNanos);
            copyLength = IOUtils.copyLarge(sourceObject, ozoneOutputStream, 0, length,
                new byte[getIOBufferSize(length)]);
            ozoneOutputStream.getMetadata()
                .putAll(sourceKeyDetails.getMetadata());
            String raw = ozoneOutputStream.getMetadata().get(OzoneConsts.ETAG);
            if (raw != null) {
              ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, stripQuotes(raw));
            }
            outputStream = ozoneOutputStream;
          }
          getMetrics().incCopyObjectSuccessLength(copyLength);
          perf.appendSizeBytes(copyLength);
        }
      } else {
        long putLength;
        try (OzoneOutputStream ozoneOutputStream = getClientProtocol()
            .createMultipartKey(volume.getName(), bucketName, key, length,
                partNumber, uploadID)) {
          metadataLatencyNs =
              getMetrics().updatePutKeyMetadataStats(startNanos);
          putLength = IOUtils.copyLarge(multiDigestInputStream, ozoneOutputStream, 0, length,
              new byte[getIOBufferSize(length)]);
          byte[] digest = multiDigestInputStream.getMessageDigest(OzoneConsts.MD5_HASH).digest();
          String md5Hash = DatatypeConverter.printHexBinary(digest).toLowerCase();
          String clientContentMD5 = getHeaders().getHeaderString(S3Consts.CHECKSUM_HEADER);
          if (clientContentMD5 != null) {
            CheckedRunnable<IOException> checkContentMD5Hook = () -> {
              S3Utils.validateContentMD5(clientContentMD5, md5Hash, key);
            };
            ozoneOutputStream.getKeyOutputStream().setPreCommits(Collections.singletonList(checkContentMD5Hook));
          }
          ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, md5Hash);
          outputStream = ozoneOutputStream;
        }
        getMetrics().incPutKeySuccessLength(putLength);
        perf.appendSizeBytes(putLength);
      }
      perf.appendMetaLatencyNanos(metadataLatencyNs);

      OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
          outputStream.getCommitUploadPartInfo();
      String eTag = omMultipartCommitUploadPartInfo.getETag();
      // If the OmMultipartCommitUploadPartInfo does not contain eTag,
      // fall back to MPU part name for compatibility in case the (old) OM
      // does not return the eTag field
      if (StringUtils.isEmpty(eTag)) {
        eTag = omMultipartCommitUploadPartInfo.getPartName();
      }
      eTag = wrapInQuotes(eTag);

      if (copyHeader != null) {
        getMetrics().updateCopyObjectSuccessStats(startNanos);
        return Response.ok(new CopyPartResult(eTag)).build();
      } else {
        getMetrics().updateCreateMultipartKeySuccessStats(startNanos);
        return Response.ok().header(HttpHeaders.ETAG, eTag).build();
      }

    } catch (OMException ex) {
      if (copyHeader != null) {
        getMetrics().updateCopyObjectFailureStats(startNanos);
      } else {
        getMetrics().updateCreateMultipartKeyFailureStats(startNanos);
      }
      if (ex.getResult() == OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadID, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, bucketName + "/" + key, ex);
      } else if (ex.getResult() == OMException.ResultCodes.INVALID_PART) {
        OS3Exception os3Exception = newError(
            S3ErrorTable.INVALID_ARGUMENT, String.valueOf(partNumber), ex);
        os3Exception.setErrorMessage(ex.getMessage());
        throw os3Exception;
      }
      throw ex;
    } finally {
      // Reset the thread-local message digest instance in case of exception
      // and MessageDigest#digest is never called
      if (multiDigestInputStream != null) {
        multiDigestInputStream.resetDigests();
      }
    }
  }

}
