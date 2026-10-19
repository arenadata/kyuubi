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
 */

package org.apache.kyuubi.util;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/** Loads the key stores and trust stores of the TLS connections. */
public final class KeyStoreUtils {

  private KeyStoreUtils() {}

  /**
   * Loads a key store or trust store file.
   *
   * @param path the file to load
   * @param type the store type, the JVM default type when null or empty
   * @param password the store password, null to load a trust store without an integrity check
   * @throws java.security.KeyStoreException if the store type is not supported
   * @throws IOException if the file cannot be read, or the password or the type is wrong
   */
  public static KeyStore load(String path, String type, String password)
      throws IOException, GeneralSecurityException {
    KeyStore store =
        KeyStore.getInstance(
            type == null || type.trim().isEmpty() ? KeyStore.getDefaultType() : type);
    try (InputStream in = Files.newInputStream(Paths.get(path))) {
      store.load(in, password == null ? null : password.toCharArray());
    }
    return store;
  }
}
