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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED_DEFAULT;

import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.http.HttpStatus;

class CreateDirectoryHandler extends ObjectOperationHandler {

  @Override
  Response handlePutRequest(ObjectRequestContext context, String keyName, InputStream body)
      throws IOException, OS3Exception {
    if (context.ignore(getAction(context, keyName))) {
      return null;
    }

    try {
      getClientProtocol().createDirectory(context.getVolume().getName(), context.getBucket().getName(), keyName);
      context.getPerf().appendMetaLatencyNanos(getMetrics().updatePutKeyMetadataStats(context.getStartNanos()));
      return Response.ok().status(HttpStatus.SC_OK).build();
    } catch (Exception e) {
      // FIXME add more specific metric?
      getMetrics().updateCreateKeyFailureStats(context.getStartNanos());
      throw e;
    }
  }

  private S3GAction getAction(ObjectRequestContext context, String keyName) throws IOException {
    if (isDirectoryCreationEnabled() && keyName.endsWith("/") && HttpMethod.PUT.equals(getContext().getMethod())
        && isZeroLength()) {
      OzoneBucket bucket = context.getBucket();
      if (bucket != null && bucket.getBucketLayout() == BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        return S3GAction.CREATE_DIRECTORY;
      }
    }
    return null;
  }

  private boolean isDirectoryCreationEnabled() {
    return getOzoneConfiguration()
        .getBoolean(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED_DEFAULT);
  }

  private boolean isZeroLength() {
    if (0 == headerParams().getLong(HttpHeaders.CONTENT_LENGTH, 0)) {
      return true;
    }
    final String length = getHeaders().getHeaderString(S3Consts.DECODED_CONTENT_LENGTH_HEADER);
    return length != null && Long.parseLong(length) == 0;
  }
}
