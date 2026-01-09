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
import static org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED_DEFAULT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ENTITY_TOO_SMALL;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_UPLOAD;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.newError;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ACCEPT_RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CONTENT_RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.MP_PARTS_COUNT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.RANGE_HEADER_SUPPORTED_UNIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_COUNT_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.stripQuotes;
import static org.apache.hadoop.ozone.s3.util.S3Utils.validateSignatureHeader;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapInQuotes;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.MultiDigestInputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.RFC1123Util;
import org.apache.hadoop.ozone.s3.util.RangeHeader;
import org.apache.hadoop.ozone.s3.util.RangeHeaderParserUtil;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.ozone.s3.util.S3StorageType;
import org.apache.hadoop.ozone.s3.util.S3Utils;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;
import org.apache.ratis.util.function.CheckedRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Key level rest endpoints.
 */
@Path("/{bucket}/{path:.+}")
public class ObjectEndpoint extends ObjectOperationHandler {

  private static final String BUCKET = "bucket";
  private static final String PATH = "path";

  private static final Logger LOG =
      LoggerFactory.getLogger(ObjectEndpoint.class);

  /*FOR the feature Overriding Response Header
  https://docs.aws.amazon.com/de_de/AmazonS3/latest/API/API_GetObject.html */
  private final Map<String, String> overrideQueryParameter;

  private ObjectOperationHandler handler;

  public ObjectEndpoint() {
    overrideQueryParameter = ImmutableMap.<String, String>builder()
        .put(HttpHeaders.CONTENT_TYPE, "response-content-type")
        .put(HttpHeaders.CONTENT_LANGUAGE, "response-content-language")
        .put(HttpHeaders.EXPIRES, "response-expires")
        .put(HttpHeaders.CACHE_CONTROL, "response-cache-control")
        .put(HttpHeaders.CONTENT_DISPOSITION, "response-content-disposition")
        .put(HttpHeaders.CONTENT_ENCODING, "response-content-encoding")
        .build();
  }

  @Override
  protected void init() {
    super.init();
    ObjectOperationHandler composite = CompositeObjectOperationHandler.newBuilder(this)
        .add(new ObjectAclHandler())
        .add(new ObjectTaggingHandler())
        .add(new MultipartKeyHandler())
        .add(new CopyKeyHandler())
        .add(new CreateDirectoryHandler())
        .add(this)
        .build();
    handler = new AuditingObjectOperationHandler(composite).copyDependenciesFrom(this);
  }

  /**
   * Rest endpoint to upload object to a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html for
   * more details.
   */
  @PUT
  public Response put(
      @PathParam(BUCKET) String bucketName,
      @PathParam(PATH) String keyPath,
      final InputStream body
  ) throws IOException, OS3Exception {
    ObjectRequestContext context = new ObjectRequestContext(S3GAction.CREATE_KEY, bucketName);
    try {
      return handler.handlePutRequest(context, keyPath, body);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.NOT_A_FILE) {
        OS3Exception os3Exception = newError(INVALID_REQUEST, keyPath, ex);
        os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
            "when calling the PutObject/MPU PartUpload operation: " +
            OmConfig.Keys.ENABLE_FILESYSTEM_PATHS + " is enabled Keys are" +
            " considered as Unix Paths. Path has Violated FS Semantics " +
            "which caused put operation to fail.");
        throw os3Exception;
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.QUOTA_EXCEEDED) {
        throw newError(S3ErrorTable.QUOTA_EXCEEDED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.FILE_ALREADY_EXISTS) {
        throw newError(S3ErrorTable.NO_OVERWRITE, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_REQUEST) {
        throw S3ErrorTable.newError(S3ErrorTable.INVALID_REQUEST, keyPath);
      } else if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_KEY, keyPath);
      } else if (ex.getResult() == ResultCodes.NOT_SUPPORTED_OPERATION) {
        // When putObjectTagging operation is applied on FSO directory
        throw S3ErrorTable.newError(S3ErrorTable.NOT_IMPLEMENTED, keyPath);
      }
      throw ex;
    }
  }

  private S3GAction getAction() {
    switch (getContext().getMethod()) {
    case HttpMethod.DELETE:
      return S3GAction.DELETE_KEY;
    case HttpMethod.HEAD:
      return S3GAction.HEAD_KEY;
    case HttpMethod.GET:
      return S3GAction.GET_KEY;
    case HttpMethod.PUT:
      return S3GAction.CREATE_KEY;
    default:
      return null;
    }
  }

  @Override
  Response handlePutRequest(ObjectRequestContext context, String keyPath, InputStream body)
      throws IOException, OS3Exception {
    if (context.ignore(getAction())) {
      return null;
    }

    MultiDigestInputStream multiDigestInputStream = null;
    try {
      long length = headerParams().getLong(HttpHeaders.CONTENT_LENGTH, 0);

      OzoneBucket bucket = context.getBucket();
      ReplicationConfig replicationConfig = getReplicationConfig(bucket);

      boolean enableEC = false;
      if ((replicationConfig != null &&
          replicationConfig.getReplicationType() == EC) ||
          bucket.getReplicationConfig() instanceof ECReplicationConfig) {
        enableEC = true;
      }

      String amzDecodedLength =
          getHeaders().getHeaderString(S3Consts.DECODED_CONTENT_LENGTH_HEADER);

      // Normal put object
      S3ChunkInputStreamInfo chunkInputStreamInfo = getS3ChunkInputStreamInfo(body,
          length, amzDecodedLength, keyPath);
      multiDigestInputStream = chunkInputStreamInfo.getMultiDigestInputStream();
      length = chunkInputStreamInfo.getEffectiveLength();

      Map<String, String> customMetadata =
          getCustomMetadataFromHeaders(getHeaders().getRequestHeaders());
      Map<String, String> tags = getTaggingFromHeaders(getHeaders());

      long putLength;
      final String md5Hash;
      if (isDatastreamEnabled() && !enableEC && length > getDatastreamMinLength()) {
        context.getPerf().appendStreamMode();
        Pair<String, Long> keyWriteResult = ObjectEndpointStreaming.put(
            bucket, keyPath, length, replicationConfig, getChunkSize(),
            customMetadata, tags, multiDigestInputStream, getHeaders(), signatureInfo.isSignPayload(),
            context.getPerf());
        md5Hash = keyWriteResult.getKey();
        putLength = keyWriteResult.getValue();
      } else {
        final String amzContentSha256Header =
            validateSignatureHeader(getHeaders(), keyPath, signatureInfo.isSignPayload());
        String volumeName = context.getVolume().getName();
        String bucketName = bucket.getName();
        try (OzoneOutputStream output = getClientProtocol()
            .createKey(volumeName, bucketName, keyPath, length, replicationConfig, customMetadata, tags)) {
          long metadataLatencyNs =
              getMetrics().updatePutKeyMetadataStats(context.getStartNanos());
          context.getPerf().appendMetaLatencyNanos(metadataLatencyNs);
          putLength = IOUtils.copyLarge(multiDigestInputStream, output, 0, length,
              new byte[getIOBufferSize(length)]);
          md5Hash = DatatypeConverter.printHexBinary(
                  multiDigestInputStream.getMessageDigest(OzoneConsts.MD5_HASH).digest())
              .toLowerCase();
          output.getMetadata().put(OzoneConsts.ETAG, md5Hash);

          List<CheckedRunnable<IOException>> preCommits = new ArrayList<>();

          String clientContentMD5 = getHeaders().getHeaderString(S3Consts.CHECKSUM_HEADER);
          if (clientContentMD5 != null) {
            CheckedRunnable<IOException> checkContentMD5Hook = () -> {
              S3Utils.validateContentMD5(clientContentMD5, md5Hash, keyPath);
            };
            preCommits.add(checkContentMD5Hook);
          }

          // If sha256Digest exists, this request must validate x-amz-content-sha256
          MessageDigest sha256Digest = multiDigestInputStream.getMessageDigest(OzoneConsts.FILE_HASH);
          if (sha256Digest != null) {
            final String actualSha256 = DatatypeConverter.printHexBinary(
                sha256Digest.digest()).toLowerCase();
            CheckedRunnable<IOException> checkSha256Hook = () -> {
              if (!amzContentSha256Header.equals(actualSha256)) {
                throw S3ErrorTable.newError(S3ErrorTable.X_AMZ_CONTENT_SHA256_MISMATCH, keyPath);
              }
            };
            preCommits.add(checkSha256Hook);
          }
          output.getKeyOutputStream().setPreCommits(preCommits);
        }
      }
      getMetrics().incPutKeySuccessLength(putLength);
      context.getPerf().appendSizeBytes(putLength);
      long opLatencyNs = getMetrics().updateCreateKeySuccessStats(context.getStartNanos());
      context.getPerf().appendOpLatencyNanos(opLatencyNs);
      return Response.ok()
          .header(HttpHeaders.ETAG, wrapInQuotes(md5Hash))
          .status(HttpStatus.SC_OK)
          .build();
    } catch (IOException | RuntimeException e) {
      getMetrics().updateCreateKeyFailureStats(context.getStartNanos());
      throw e;
    } finally {
      // Reset the thread-local message digest instance in case of exception
      // and MessageDigest#digest is never called
      if (multiDigestInputStream != null) {
        multiDigestInputStream.resetDigests();
      }
    }
  }

  /**
   * Rest endpoint to download object from a bucket, if query param uploadId
   * is specified, request for list parts of a multipart upload key with
   * specific uploadId.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html
   * https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
   * for more details.
   */
  @SuppressWarnings("checkstyle:MethodLength")
  @GET
  public Response get(
      @PathParam(BUCKET) String bucketName,
      @PathParam(PATH) String keyPath
  ) throws IOException, OS3Exception {
    final int maxParts = queryParams().getInt(QueryParams.MAX_PARTS, 1000);
    final int partNumber = queryParams().getInt(QueryParams.PART_NUMBER, 0);
    final String partNumberMarker = queryParams().get(QueryParams.PART_NUMBER_MARKER);
    final String taggingMarker = queryParams().get(QueryParams.TAGGING);
    final String uploadId = queryParams().get(QueryParams.UPLOAD_ID);

    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.GET_KEY;
    PerformanceStringBuilder perf = new PerformanceStringBuilder();
    try {
      OzoneBucket bucket = getBucket(bucketName);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());

      if (taggingMarker != null) {
        s3GAction = S3GAction.GET_OBJECT_TAGGING;
        return getObjectTagging(bucket, keyPath);
      }

      if (uploadId != null) {
        // When we have uploadId, this is the request for list Parts.
        s3GAction = S3GAction.LIST_PARTS;
        int partMarker = parsePartNumberMarker(partNumberMarker);
        Response response = listParts(bucket, keyPath, uploadId,
            partMarker, maxParts, perf);
        auditReadSuccess(s3GAction, perf);
        return response;
      }

      OzoneKeyDetails keyDetails = (partNumber != 0) ?
          getClientProtocol().getS3KeyDetails(bucketName, keyPath, partNumber) :
          getClientProtocol().getS3KeyDetails(bucketName, keyPath);

      isFile(keyPath, keyDetails);

      long length = keyDetails.getDataSize();

      LOG.debug("Data length of the key {} is {}", keyPath, length);

      String rangeHeaderVal = getHeaders().getHeaderString(RANGE_HEADER);
      RangeHeader rangeHeader = null;

      LOG.debug("range Header provided value: {}", rangeHeaderVal);

      if (rangeHeaderVal != null) {
        rangeHeader = RangeHeaderParserUtil.parseRangeHeader(rangeHeaderVal,
            length);
        LOG.debug("range Header provided: {}", rangeHeader);
        if (rangeHeader.isInValidRange()) {
          throw newError(S3ErrorTable.INVALID_RANGE, rangeHeaderVal);
        }
      }
      ResponseBuilder responseBuilder;

      if (rangeHeaderVal == null || rangeHeader.isReadFull()) {
        StreamingOutput output = dest -> {
          try (OzoneInputStream key = keyDetails.getContent()) {
            long readLength = IOUtils.copy(key, dest, getIOBufferSize(keyDetails.getDataSize()));
            getMetrics().incGetKeySuccessLength(readLength);
            perf.appendSizeBytes(readLength);
          }
          long opLatencyNs =  getMetrics().updateGetKeySuccessStats(startNanos);
          perf.appendOpLatencyNanos(opLatencyNs);
          auditReadSuccess(S3GAction.GET_KEY, perf);
        };
        responseBuilder = Response
            .ok(output)
            .header(HttpHeaders.CONTENT_LENGTH, keyDetails.getDataSize());

      } else {

        long startOffset = rangeHeader.getStartOffset();
        long endOffset = rangeHeader.getEndOffset();
        // eg. if range header is given as bytes=0-0, then we should return 1
        // byte from start offset
        long copyLength = endOffset - startOffset + 1;
        StreamingOutput output = dest -> {
          try (OzoneInputStream ozoneInputStream = keyDetails.getContent()) {
            ozoneInputStream.seek(startOffset);
            long readLength = IOUtils.copyLarge(ozoneInputStream, dest, 0,
                copyLength, new byte[getIOBufferSize(copyLength)]);
            getMetrics().incGetKeySuccessLength(readLength);
            perf.appendSizeBytes(readLength);
          }
          long opLatencyNs = getMetrics().updateGetKeySuccessStats(startNanos);
          perf.appendOpLatencyNanos(opLatencyNs);
          auditReadSuccess(S3GAction.GET_KEY, perf);
        };
        responseBuilder = Response
            .status(Status.PARTIAL_CONTENT)
            .entity(output)
            .header(HttpHeaders.CONTENT_LENGTH, copyLength);

        String contentRangeVal = RANGE_HEADER_SUPPORTED_UNIT + " " +
            rangeHeader.getStartOffset() + "-" + rangeHeader.getEndOffset() +
            "/" + length;

        responseBuilder.header(CONTENT_RANGE_HEADER, contentRangeVal);
      }
      responseBuilder
          .header(ACCEPT_RANGE_HEADER, RANGE_HEADER_SUPPORTED_UNIT);

      String eTag = keyDetails.getMetadata().get(OzoneConsts.ETAG);
      if (eTag != null) {
        responseBuilder.header(HttpHeaders.ETAG, wrapInQuotes(eTag));
        String partsCount = extractPartsCount(eTag);
        if (partsCount != null) {
          responseBuilder.header(MP_PARTS_COUNT, partsCount);
        }
      }

      // if multiple query parameters having same name,
      // Only the first parameters will be recognized
      // eg:
      // http://localhost:9878/bucket/key?response-expires=1&response-expires=2
      // only response-expires=1 is valid
      MultivaluedMap<String, String> queryParams = getContext()
          .getUriInfo().getQueryParameters();

      for (Map.Entry<String, String> entry :
          overrideQueryParameter.entrySet()) {
        String headerValue = getHeaders().getHeaderString(entry.getKey());

        /* "Overriding Response Header" by query parameter, See:
        https://docs.aws.amazon.com/de_de/AmazonS3/latest/API/API_GetObject.html
        */
        String queryValue = queryParams.getFirst(entry.getValue());
        if (queryValue != null) {
          headerValue = queryValue;
        }
        if (headerValue != null) {
          responseBuilder.header(entry.getKey(), headerValue);
        }
      }
      addLastModifiedDate(responseBuilder, keyDetails);
      addTagCountIfAny(responseBuilder, keyDetails);
      long metadataLatencyNs =
          getMetrics().updateGetKeyMetadataStats(startNanos);
      perf.appendMetaLatencyNanos(metadataLatencyNs);
      return responseBuilder.build();
    } catch (OMException ex) {
      auditReadFailure(s3GAction, ex);
      if (taggingMarker != null) {
        getMetrics().updateGetObjectTaggingFailureStats(startNanos);
      } else if (uploadId != null) {
        getMetrics().updateListPartsFailureStats(startNanos);
      } else {
        getMetrics().updateGetKeyFailureStats(startNanos);
      }
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_KEY, keyPath, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      auditReadFailure(s3GAction, ex);
      throw ex;
    }
  }

  static void addLastModifiedDate(
      ResponseBuilder responseBuilder, OzoneKey key) {

    ZonedDateTime lastModificationTime = key.getModificationTime()
        .atZone(ZoneId.of(OzoneConsts.OZONE_TIME_ZONE));

    responseBuilder
        .header(HttpHeaders.LAST_MODIFIED,
            RFC1123Util.FORMAT.format(lastModificationTime));
  }

  static void addTagCountIfAny(
      ResponseBuilder responseBuilder, OzoneKey key) {
    // See x-amz-tagging-count in https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
    // The number of tags, IF ANY, on the object, when you have the relevant
    // permission to read object tags
    if (!key.getTags().isEmpty()) {
      responseBuilder
          .header(TAG_COUNT_HEADER, key.getTags().size());
    }
  }

  /**
   * Rest endpoint to check existence of an object in a bucket.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectHEAD.html
   * for more details.
   */
  @HEAD
  public Response head(
      @PathParam(BUCKET) String bucketName,
      @PathParam(PATH) String keyPath) throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.HEAD_KEY;

    OzoneKey key;
    try {
      if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
        OzoneBucket bucket = getBucket(bucketName);
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      }

      key = getClientProtocol().headS3Object(bucketName, keyPath);

      isFile(keyPath, key);
      // TODO: return the specified range bytes of this object.
    } catch (OMException ex) {
      auditReadFailure(s3GAction, ex);
      getMetrics().updateHeadKeyFailureStats(startNanos);
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        // Just return 404 with no content
        return Response.status(Status.NOT_FOUND).build();
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      auditReadFailure(s3GAction, ex);
      throw ex;
    }

    S3StorageType s3StorageType = key.getReplicationConfig() == null ?
        S3StorageType.STANDARD :
        S3StorageType.fromReplicationConfig(key.getReplicationConfig());

    ResponseBuilder response = Response.ok().status(HttpStatus.SC_OK)
        .header(HttpHeaders.CONTENT_LENGTH, key.getDataSize())
        .header(HttpHeaders.CONTENT_TYPE, "binary/octet-stream")
        .header(STORAGE_CLASS_HEADER, s3StorageType.toString());

    String eTag = key.getMetadata().get(OzoneConsts.ETAG);
    if (eTag != null) {
      // Should not return ETag header if the ETag is not set
      // doing so will result in "null" string being returned instead
      // which breaks some AWS SDK implementation
      response.header(HttpHeaders.ETAG, wrapInQuotes(eTag));
      String partsCount = extractPartsCount(eTag);
      if (partsCount != null) {
        response.header(MP_PARTS_COUNT, partsCount);
      }
    }

    addLastModifiedDate(response, key);
    addCustomMetadataHeaders(response, key);
    getMetrics().updateHeadKeySuccessStats(startNanos);
    auditReadSuccess(s3GAction);
    return response.build();
  }

  private void isFile(String keyPath, OzoneKey key) throws OMException {
    /*
      Necessary for directories in buckets with FSO layout.
      Intended for apps which use Hadoop S3A.
      Example of such app is Trino (through Hive connector).
     */
    boolean isFsoDirCreationEnabled = getOzoneConfiguration()
        .getBoolean(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED,
            OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED_DEFAULT);
    if (isFsoDirCreationEnabled &&
        !key.isFile() &&
        !keyPath.endsWith("/")) {
      throw new OMException(ResultCodes.KEY_NOT_FOUND);
    }
  }

  /**
   * Abort multipart upload request.
   * @param bucket
   * @param key
   * @param uploadId
   * @return Response
   * @throws IOException
   * @throws OS3Exception
   */
  private Response abortMultipartUpload(OzoneVolume volume, String bucket,
                                        String key, String uploadId)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    try {
      getClientProtocol().abortMultipartUpload(volume.getName(), bucket,
          key, uploadId);
    } catch (OMException ex) {
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(S3ErrorTable.NO_SUCH_UPLOAD, uploadId, ex);
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucket, ex);
      }
      throw ex;
    }
    getMetrics().updateAbortMultipartUploadSuccessStats(startNanos);
    return Response
        .status(Status.NO_CONTENT)
        .build();
  }


  /**
   * Delete a specific object from a bucket, if query param uploadId is
   * specified, this request is for abort multipart upload.
   * <p>
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html
   * https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadAbort.html
   * for more details.
   */
  @DELETE
  @SuppressWarnings("emptyblock")
  public Response delete(
      @PathParam(BUCKET) String bucketName,
      @PathParam(PATH) String keyPath
  ) throws IOException, OS3Exception {
    final String taggingMarker = queryParams().get(QueryParams.TAGGING);
    final String uploadId = queryParams().get(QueryParams.UPLOAD_ID);

    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.DELETE_KEY;

    try {
      OzoneVolume volume = getVolume();
      OzoneBucket bucket = null;
      if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
        bucket = volume.getBucket(bucketName);
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, bucket.getOwner());
      }

      if (taggingMarker != null) {
        s3GAction = S3GAction.DELETE_OBJECT_TAGGING;
        return deleteObjectTagging(volume, bucketName, keyPath);
      }

      if (uploadId != null && !uploadId.equals("")) {
        s3GAction = S3GAction.ABORT_MULTIPART_UPLOAD;
        return abortMultipartUpload(volume, bucketName, keyPath, uploadId);
      }
      getClientProtocol().deleteKey(volume.getName(), bucketName,
          keyPath, false);
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      if (uploadId != null && !uploadId.equals("")) {
        getMetrics().updateAbortMultipartUploadFailureStats(startNanos);
      } else {
        getMetrics().updateDeleteKeyFailureStats(startNanos);
      }
      if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucketName, ex);
      } else if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        //NOT_FOUND is not a problem, AWS doesn't throw exception for missing
        // keys. Just return 204
      } else if (ex.getResult() == ResultCodes.DIRECTORY_NOT_EMPTY) {
        // With PREFIX metadata layout, a dir deletion without recursive flag
        // to true will throw DIRECTORY_NOT_EMPTY error for a non-empty dir.
        // NOT_FOUND is not a problem, AWS doesn't throw exception for missing
        // keys. Just return 204
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, keyPath, ex);
      } else if (ex.getResult() == ResultCodes.NOT_SUPPORTED_OPERATION) {
        // When deleteObjectTagging operation is applied on FSO directory
        throw S3ErrorTable.newError(S3ErrorTable.NOT_IMPLEMENTED, keyPath);
      } else {
        throw ex;
      }
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      if (taggingMarker != null) {
        getMetrics().updateDeleteObjectTaggingFailureStats(startNanos);
      } else if (uploadId != null && !uploadId.equals("")) {
        getMetrics().updateAbortMultipartUploadFailureStats(startNanos);
      } else {
        getMetrics().updateDeleteKeyFailureStats(startNanos);
      }
      throw ex;
    }
    getMetrics().updateDeleteKeySuccessStats(startNanos);
    auditWriteSuccess(s3GAction);
    return Response
        .status(Status.NO_CONTENT)
        .build();
  }

  /**
   * Initialize MultiPartUpload request.
   * <p>
   * Note: the specific content type is set by the HeaderPreprocessor.
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  @Consumes(HeaderPreprocessor.MULTIPART_UPLOAD_MARKER)
  public Response initializeMultipartUpload(
      @PathParam(BUCKET) String bucket,
      @PathParam(PATH) String key
  ) throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.INIT_MULTIPART_UPLOAD;

    try {
      OzoneBucket ozoneBucket = getBucket(bucket);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucket, ozoneBucket.getOwner());

      Map<String, String> customMetadata =
          getCustomMetadataFromHeaders(getHeaders().getRequestHeaders());

      Map<String, String> tags = getTaggingFromHeaders(getHeaders());

      ReplicationConfig replicationConfig = getReplicationConfig(ozoneBucket);

      OmMultipartInfo multipartInfo =
          ozoneBucket.initiateMultipartUpload(key, replicationConfig, customMetadata, tags);

      MultipartUploadInitiateResponse multipartUploadInitiateResponse = new
          MultipartUploadInitiateResponse();

      multipartUploadInitiateResponse.setBucket(bucket);
      multipartUploadInitiateResponse.setKey(key);
      multipartUploadInitiateResponse.setUploadID(multipartInfo.getUploadID());

      auditWriteSuccess(s3GAction);
      getMetrics().updateInitMultipartUploadSuccessStats(startNanos);
      return Response.status(Status.OK).entity(
          multipartUploadInitiateResponse).build();
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().updateInitMultipartUploadFailureStats(startNanos);
      if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED, key, ex);
      }
      throw ex;
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().updateInitMultipartUploadFailureStats(startNanos);
      throw ex;
    }
  }

  /**
   * Complete a multipart upload.
   */
  @POST
  @Produces(MediaType.APPLICATION_XML)
  public Response completeMultipartUpload(
      @PathParam(BUCKET) String bucket,
      @PathParam(PATH) String key,
      CompleteMultipartUploadRequest multipartUploadRequest
  ) throws IOException, OS3Exception {
    final String uploadID = queryParams().get(QueryParams.UPLOAD_ID, "");
    long startNanos = Time.monotonicNowNanos();
    S3GAction s3GAction = S3GAction.COMPLETE_MULTIPART_UPLOAD;
    OzoneVolume volume = getVolume();
    // Using LinkedHashMap to preserve ordering of parts list.
    Map<Integer, String> partsMap = new LinkedHashMap<>();
    List<CompleteMultipartUploadRequest.Part> partList =
        multipartUploadRequest.getPartList();

    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo;
    try {
      OzoneBucket ozoneBucket = volume.getBucket(bucket);
      S3Owner.verifyBucketOwnerCondition(getHeaders(), bucket, ozoneBucket.getOwner());

      for (CompleteMultipartUploadRequest.Part part : partList) {
        partsMap.put(part.getPartNumber(), stripQuotes(part.getETag()));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Parts map {}", partsMap);
      }

      omMultipartUploadCompleteInfo = ozoneBucket.completeMultipartUpload(key, uploadID, partsMap);
      CompleteMultipartUploadResponse completeMultipartUploadResponse =
          new CompleteMultipartUploadResponse();
      completeMultipartUploadResponse.setBucket(bucket);
      completeMultipartUploadResponse.setKey(key);
      completeMultipartUploadResponse.setETag(
          wrapInQuotes(omMultipartUploadCompleteInfo.getHash()));
      // Location also setting as bucket name.
      completeMultipartUploadResponse.setLocation(bucket);
      auditWriteSuccess(s3GAction);
      getMetrics().updateCompleteMultipartUploadSuccessStats(startNanos);
      return Response.status(Status.OK).entity(completeMultipartUploadResponse)
          .build();
    } catch (OMException ex) {
      auditWriteFailure(s3GAction, ex);
      getMetrics().updateCompleteMultipartUploadFailureStats(startNanos);
      if (ex.getResult() == ResultCodes.INVALID_PART) {
        throw newError(S3ErrorTable.INVALID_PART, key, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_PART_ORDER) {
        throw newError(S3ErrorTable.INVALID_PART_ORDER, key, ex);
      } else if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadID, ex);
      } else if (ex.getResult() == ResultCodes.ENTITY_TOO_SMALL) {
        throw newError(ENTITY_TOO_SMALL, key, ex);
      } else if (ex.getResult() == ResultCodes.INVALID_REQUEST) {
        OS3Exception os3Exception = newError(INVALID_REQUEST, key, ex);
        os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
            "when calling the CompleteMultipartUpload operation: You must " +
            "specify at least one part");
        throw os3Exception;
      } else if (ex.getResult() == ResultCodes.NOT_A_FILE) {
        OS3Exception os3Exception = newError(INVALID_REQUEST, key, ex);
        os3Exception.setErrorMessage("An error occurred (InvalidRequest) " +
            "when calling the CompleteMultipartUpload operation: " +
            OmConfig.Keys.ENABLE_FILESYSTEM_PATHS + " is enabled Keys are " +
            "considered as Unix Paths. A directory already exists with a " +
            "given KeyName caused failure for MPU");
        throw os3Exception;
      } else if (ex.getResult() == ResultCodes.BUCKET_NOT_FOUND) {
        throw newError(S3ErrorTable.NO_SUCH_BUCKET, bucket, ex);
      }
      throw ex;
    } catch (Exception ex) {
      auditWriteFailure(s3GAction, ex);
      throw ex;
    }
  }

  /**
   * Returns response for the listParts request.
   * See: https://docs.aws.amazon.com/AmazonS3/latest/API/mpUploadListParts.html
   * @param ozoneBucket
   * @param key
   * @param uploadID
   * @param partNumberMarker
   * @param maxParts
   * @return
   * @throws IOException
   * @throws OS3Exception
   */
  private Response listParts(OzoneBucket ozoneBucket, String key, String uploadID,
      int partNumberMarker, int maxParts, PerformanceStringBuilder perf)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();
    ListPartsResponse listPartsResponse = new ListPartsResponse();
    String bucketName = ozoneBucket.getName();
    try {
      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
          ozoneBucket.listParts(key, uploadID, partNumberMarker, maxParts);
      listPartsResponse.setBucket(bucketName);
      listPartsResponse.setKey(key);
      listPartsResponse.setUploadID(uploadID);
      listPartsResponse.setMaxParts(maxParts);
      listPartsResponse.setPartNumberMarker(partNumberMarker);
      listPartsResponse.setTruncated(false);

      listPartsResponse.setStorageClass(S3StorageType.fromReplicationConfig(
          ozoneMultipartUploadPartListParts.getReplicationConfig()).toString());

      if (ozoneMultipartUploadPartListParts.isTruncated()) {
        listPartsResponse.setTruncated(
            ozoneMultipartUploadPartListParts.isTruncated());
        listPartsResponse.setNextPartNumberMarker(
            ozoneMultipartUploadPartListParts.getNextPartNumberMarker());
      }

      ozoneMultipartUploadPartListParts.getPartInfoList().forEach(partInfo -> {
        ListPartsResponse.Part part = new ListPartsResponse.Part();
        part.setPartNumber(partInfo.getPartNumber());
        // If the ETag field does not exist, use MPU part name for backward
        // compatibility
        part.setETag(StringUtils.isNotEmpty(partInfo.getETag()) ?
            partInfo.getETag() : partInfo.getPartName());
        part.setSize(partInfo.getSize());
        part.setLastModified(Instant.ofEpochMilli(
            partInfo.getModificationTime()));
        listPartsResponse.addPart(part);
      });
    } catch (OMException ex) {
      getMetrics().updateListPartsFailureStats(startNanos);
      if (ex.getResult() == ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR) {
        throw newError(NO_SUCH_UPLOAD, uploadID, ex);
      } else if (isAccessDenied(ex)) {
        throw newError(S3ErrorTable.ACCESS_DENIED,
            bucketName + "/" + key + "/" + uploadID, ex);
      }
      throw ex;
    }
    long opLatencyNs = getMetrics().updateListPartsSuccessStats(startNanos);
    perf.appendCount(listPartsResponse.getPartList().size());
    perf.appendOpLatencyNanos(opLatencyNs);
    return Response.status(Status.OK).entity(listPartsResponse).build();
  }

  private Response getObjectTagging(OzoneBucket bucket, String keyName) throws IOException {
    long startNanos = Time.monotonicNowNanos();

    Map<String, String> tagMap = bucket.getObjectTagging(keyName);

    getMetrics().updateGetObjectTaggingSuccessStats(startNanos);
    return Response.ok(S3Tagging.fromMap(tagMap), MediaType.APPLICATION_XML_TYPE).build();
  }

  private Response deleteObjectTagging(OzoneVolume volume, String bucketName, String keyName)
      throws IOException, OS3Exception {
    long startNanos = Time.monotonicNowNanos();

    try {
      volume.getBucket(bucketName).deleteObjectTagging(keyName);
    } catch (OMException ex) {
      // Unlike normal key deletion that ignores the key not found exception
      // DeleteObjectTagging should throw the exception if the key does not exist
      if (ex.getResult() == ResultCodes.KEY_NOT_FOUND) {
        throw S3ErrorTable.newError(S3ErrorTable.NO_SUCH_KEY, keyName);
      }
      throw ex;
    }

    getMetrics().updateDeleteObjectTaggingSuccessStats(startNanos);
    return Response.noContent().build();
  }

  final class ObjectRequestContext {
    private final String bucketName;
    private final long startNanos;
    private final PerformanceStringBuilder perf;
    private S3GAction action;
    private OzoneVolume volume;
    private OzoneBucket bucket;

    /** @param action best guess on action based on request method, may be refined later by handlers */
    ObjectRequestContext(S3GAction action, String bucketName) {
      this.action = action;
      this.bucketName = bucketName;
      this.startNanos = Time.monotonicNowNanos();
      this.perf = new PerformanceStringBuilder();
    }

    long getStartNanos() {
      return startNanos;
    }

    PerformanceStringBuilder getPerf() {
      return perf;
    }

    String getBucketName() {
      return bucketName;
    }

    OzoneVolume getVolume() throws IOException {
      if (volume == null) {
        volume = ObjectEndpoint.this.getVolume();
      }
      return volume;
    }

    OzoneBucket getBucket() throws IOException {
      if (bucket == null) {
        bucket = getVolume().getBucket(bucketName);
      }
      return bucket;
    }

    void verifyBucketOwner() throws IOException {
      if (S3Owner.hasBucketOwnershipVerificationConditions(getHeaders())) {
        S3Owner.verifyBucketOwnerCondition(getHeaders(), bucketName, getBucket().getOwner());
      }
    }

    S3GAction getAction() {
      return action;
    }

    /** Allow handler to set more specific action. */
    boolean ignore(S3GAction a) {
      final boolean ignore = a == null;
      if (!ignore) {
        this.action = a;
      }
      return ignore;
    }
  }
}
