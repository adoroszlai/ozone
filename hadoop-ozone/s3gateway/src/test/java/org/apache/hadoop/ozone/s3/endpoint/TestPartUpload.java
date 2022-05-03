/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

/**
 * This class tests Upload part request.
 */
class TestPartUpload {

  private final ObjectEndpoint subject = new ObjectEndpoint();

  @BeforeEach
  void setUp() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER))
        .thenReturn("STANDARD");

    subject.setHeaders(headers);
    subject.setClient(client);
    subject.setOzoneConfiguration(new OzoneConfiguration());
  }

  @ParameterizedTest
  @ArgumentsSource(KeyContentProvider.class)
  void testPartWithRealLength(byte[] content) throws Exception {
    String uploadID = initiateMultipartUpload();

    assertSuccess(uploadPart(uploadID, 1, content));
    assertSuccess(uploadPart(uploadID, 2, content));
  }

  @ParameterizedTest
  @ArgumentsSource(KeyContentProvider.class)
  void testPartWithoutLength(byte[] content) throws Exception {
    String uploadID = initiateMultipartUpload();

    assertSuccess(uploadPart(uploadID, 1, content, 0));
    assertSuccess(uploadPart(uploadID, 2, content, 0));
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 1})
  void rejectsPartWithWrongContentLength(int diff) throws Exception {
    String uploadID = initiateMultipartUpload();
    byte[] content = RandomUtils.nextBytes(100);

    try {
      uploadPart(uploadID, 1, content, content.length + diff);
      fail("Expected upload with wrong content length to fail");
    } catch (OS3Exception e) {
      // TODO confirm expected error code
      assertEquals(S3ErrorTable.INVALID_REQUEST.getCode(), e.getCode());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(KeyContentProvider.class)
  void testPartWithOverride(byte[] content) throws Exception {
    String uploadID = initiateMultipartUpload();
    String eTag = assertSuccess(uploadPart(uploadID, 1, content));

    // Upload part again with same part Number
    byte[] newContent = "new content".getBytes(UTF_8);
    String newETag = assertSuccess(uploadPart(uploadID, 1, newContent));

    // ETag should be changed
    assertNotEquals(eTag, newETag);
  }

  @Test
  void rejectsPartWithUnknownUploadID() throws Exception {
    try {
      byte[] content = "With Incorrect uploadID".getBytes(UTF_8);
      uploadPart("unknown", 1, content);
      fail("Expected upload-part to fail due to unknown upload ID");
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_UPLOAD.getCode(), ex.getCode());
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
    }
  }

  private String initiateMultipartUpload() throws IOException, OS3Exception {
    Response response = subject.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        OzoneConsts.KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    assertEquals(200, response.getStatus());
    return multipartUploadInitiateResponse.getUploadID();
  }

  private Response uploadPart(String uploadID, int partNumber,
      byte[] content) throws IOException, OS3Exception {
    return uploadPart(uploadID, partNumber, content, content.length);
  }

  private Response uploadPart(String uploadID, int partNumber, byte[] content,
      long length) throws IOException, OS3Exception {
    return subject.put(OzoneConsts.S3_BUCKET, OzoneConsts.KEY,
        length, partNumber, uploadID,
        new ByteArrayInputStream(content));
  }

  /**
   * Verify {@code response} is a successful part upload.
   * @return ETag from response
   */
  private String assertSuccess(Response response) {
    assertEquals(200, response.getStatus());

    String eTag = response.getHeaderString("ETag");
    assertNotNull(eTag);

    return eTag;
  }

}
