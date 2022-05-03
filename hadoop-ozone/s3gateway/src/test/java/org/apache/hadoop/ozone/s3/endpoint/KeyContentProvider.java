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
package org.apache.hadoop.ozone.s3.endpoint;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;
import static org.apache.commons.lang3.RandomUtils.nextBytes;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Source of key content test cases.
 */
public final class KeyContentProvider implements ArgumentsProvider {

  @Override
  public Stream<Arguments> provideArguments(ExtensionContext context) {
    return Stream.of(
        arguments(Named.of("ASCII", randomAscii(100).getBytes(UTF_8))),
        arguments(Named.of("latin2", "árvíztűrő tükörfúrógép".getBytes(UTF_8))),
        arguments(Named.of("binary", nextBytes(100)))
    );
  }
}
