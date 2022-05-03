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

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

/**
 * Test put object.
 */
class TestObjectPut {

  private static final byte[] CONTENT = RandomUtils.nextBytes(10);
  private static final String BUCKET_NAME = "b1";
  private static final String KEY_NAME = "key=value/1";
  private static final String DEST_BUCKET = "b2";

  private OzoneClient clientStub;
  private ObjectEndpoint objectEndpoint;
  private HttpHeaders headers;

  @BeforeEach
  void setup() throws IOException {
    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(BUCKET_NAME);
    clientStub.getObjectStore().createS3Bucket(DEST_BUCKET);

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(clientStub);
    objectEndpoint.setOzoneConfiguration(new OzoneConfiguration());

    headers = Mockito.mock(HttpHeaders.class);
    objectEndpoint.setHeaders(headers);
  }

  @ParameterizedTest
  @ArgumentsSource(KeyContentProvider.class)
  void testPutObject(byte[] content) throws IOException, OS3Exception {
    assertSuccess(putObject(BUCKET_NAME, KEY_NAME, content, content.length));
    assertKeyContent(BUCKET_NAME, KEY_NAME, content);
  }

  @ParameterizedTest
  @ArgumentsSource(KeyContentProvider.class)
  void testPutObjectWithoutLength(byte[] content)
      throws IOException, OS3Exception {
    assertSuccess(putObject(BUCKET_NAME, KEY_NAME, content, 0));
    assertKeyContent(BUCKET_NAME, KEY_NAME, content);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 1})
  void rejectsPutObjectWithWrongContentLength(int diff)
      throws IOException {
    byte[] content = RandomUtils.nextBytes(100);
    try {
      putObject(BUCKET_NAME, KEY_NAME, content, content.length + diff);
      fail("Expected upload with wrong content length to fail");
    } catch (OS3Exception e) {
      // TODO confirm expected error code
      assertEquals(S3ErrorTable.INVALID_REQUEST.getCode(), e.getCode());
    }
  }

  @ParameterizedTest
  @ArgumentsSource(KeyContentProvider.class)
  void testPutObjectWithECReplicationConfig(byte[] content)
      throws IOException, OS3Exception {
    //GIVEN
    ECReplicationConfig ecReplicationConfig =
        new ECReplicationConfig("rs-3-2-1024K");
    clientStub.getObjectStore().getS3Bucket(BUCKET_NAME)
        .setReplicationConfig(ecReplicationConfig);

    // WHEN
    Response response = putObject(BUCKET_NAME, KEY_NAME,
        content, content.length);

    // THEN
    assertSuccess(response);
    assertEquals(ecReplicationConfig,
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME)
            .getReplicationConfig());
    assertKeyContent(BUCKET_NAME, KEY_NAME, content);
  }

  @Test
  void testPutObjectWithSignedChunks() throws IOException, OS3Exception {
    //GIVEN
    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";
    byte[] bytes = chunkedContent.getBytes(UTF_8);
    when(headers.getHeaderString("x-amz-content-sha256"))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");

    //WHEN
    Response response = putObject(BUCKET_NAME, KEY_NAME, bytes, 0);

    //THEN
    assertSuccess(response);
    assertKeyContent(BUCKET_NAME, KEY_NAME, "1234567890abcde".getBytes(UTF_8));
  }

  @ParameterizedTest
  @ArgumentsSource(KeyContentProvider.class)
  void testCopyObject(byte[] content) throws IOException, OS3Exception {
    // GIVEN
    putObject(BUCKET_NAME, KEY_NAME, content, content.length);
    when(headers.getHeaderString(COPY_SOURCE_HEADER))
        .thenReturn(BUCKET_NAME + "/" + urlEncode(KEY_NAME));

    // WHEN
    String destKey = "key=value/2";
    Response response = putObject(DEST_BUCKET, destKey,
        content, content.length);

    // THEN
    assertSuccess(response);
    assertKeyContent(DEST_BUCKET, destKey, content);
  }

  @Test
  void rejectsCopyToSameLocation() throws IOException, OS3Exception {
    // GIVEN
    putObject(BUCKET_NAME, KEY_NAME, CONTENT, CONTENT.length);
    when(headers.getHeaderString(COPY_SOURCE_HEADER))
        .thenReturn(BUCKET_NAME + "/" + urlEncode(KEY_NAME));

    // WHEN + THEN
    try {
      putObject(BUCKET_NAME, KEY_NAME, CONTENT, CONTENT.length);
      fail("Expected copy to fail when source and destination is the same");
    } catch (OS3Exception ex) {
      assertTrue(ex.getErrorMessage().contains("This copy request is illegal"));
    }
  }

  @Test
  void copyFromNonExistentBucket() throws IOException {
    // GIVEN
    when(headers.getHeaderString(COPY_SOURCE_HEADER))
        .thenReturn("no-such-bucket" + "/" + urlEncode(KEY_NAME));

    try {
      // WHEN
      putObject(DEST_BUCKET, KEY_NAME, CONTENT, CONTENT.length);
      fail("Expected copy from non-existent source bucket to fail");
    } catch (OS3Exception ex) {
      // THEN
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
  }

  @Test
  void copyToNonExistentBucket() throws IOException {
    // GIVEN
    when(headers.getHeaderString(COPY_SOURCE_HEADER))
        .thenReturn(BUCKET_NAME + "/" + urlEncode(KEY_NAME));

    try {
      // WHEN
      putObject("no-such-bucket", KEY_NAME, CONTENT, CONTENT.length);
      fail("Expected copy to non-existent bucket to fail");
    } catch (OS3Exception ex) {
      // THEN
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
  }

  @Test
  void copyFromNonExistentToNonExistent() throws IOException {
    // GIVEN
    when(headers.getHeaderString(COPY_SOURCE_HEADER))
        .thenReturn("no-such-bucket" + "/" + urlEncode(KEY_NAME));

    try {
      // WHEN
      putObject("other-missing-bucket", "otherKey", CONTENT, CONTENT.length);
      fail("Expected copy to fail when neither source nor destination exist");
    } catch (OS3Exception ex) {
      // THEN
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
  }

  @Test
  void copyNonExistentKey() throws IOException {
    // GIVEN
    when(headers.getHeaderString(COPY_SOURCE_HEADER))
        .thenReturn(BUCKET_NAME + "/" + urlEncode("no-such-key"));

    try {
      // WHEN
      putObject(DEST_BUCKET, KEY_NAME, CONTENT, CONTENT.length);
      fail("Expected copy from non-existent source key to fail");
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_KEY.getCode(), ex.getCode());
    }
  }

  @Test
  void rejectsInvalidStorageClass() throws IOException {
    when(headers.getHeaderString(STORAGE_CLASS_HEADER))
        .thenReturn("random");

    try {
      putObject(BUCKET_NAME, KEY_NAME, CONTENT, CONTENT.length);
      fail("Expected to fail with invalid storage type");
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.INVALID_ARGUMENT.getErrorMessage(),
          ex.getErrorMessage());
      assertEquals("random", ex.getResource());
    }
  }

  @Test
  void usesDefaultReplicationForEmptyStorageClass()
      throws IOException, OS3Exception {
    // GIVEN
    when(headers.getHeaderString(STORAGE_CLASS_HEADER))
        .thenReturn("");

    // WHEN
    Response response = putObject(BUCKET_NAME, KEY_NAME,
        CONTENT, CONTENT.length);

    // THEN
    assertSuccess(response);

    OzoneKeyDetails key =
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME)
            .getKey(KEY_NAME);
    //default type is set
    assertEquals(ReplicationType.RATIS, key.getReplicationType());
  }

  private Response putObject(String bucket, String key, byte[] content,
      long length) throws IOException, OS3Exception {
    return objectEndpoint.put(bucket, key, length,
        0, null, new ByteArrayInputStream(content));
  }

  private static void assertSuccess(Response response) {
    assertEquals(200, response.getStatus());
  }

  private void assertKeyContent(String bucket, String key,
      byte[] expectedContent) throws IOException {
    OzoneInputStream ozoneInputStream =
        clientStub.getObjectStore().getS3Bucket(bucket)
            .readKey(key);
    byte[] keyContent = new byte[expectedContent.length];
    IOUtils.readFully(ozoneInputStream, keyContent);
    assertArrayEquals(expectedContent, keyContent);
  }
}
