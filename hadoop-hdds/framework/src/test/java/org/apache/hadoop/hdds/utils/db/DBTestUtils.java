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
package org.apache.hadoop.hdds.utils.db;

import com.google.common.collect.Iterators;

import java.io.IOException;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Utilities for testing DB-related code.
 */
public final class DBTestUtils {

  public static void assertEmpty(Table<?, ?> table) throws IOException {
    Iterator<?> cacheIterator = table.cacheIterator();
    assertFalse(cacheIterator.hasNext(),
        () -> "Unexpectedly found in table cache: "
            + Iterators.toString(cacheIterator));

    try (TableIterator<?, ?> it = table.iterator()) {
      assertFalse(it.hasNext(),
          () -> "Unexpectedly found in table: " + Iterators.toString(it));
    }

    assertTrue(table.isEmpty());
  }

}
