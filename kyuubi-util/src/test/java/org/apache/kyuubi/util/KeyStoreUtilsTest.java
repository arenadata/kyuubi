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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class KeyStoreUtilsTest {

  private static final String PASSWORD = "kyuubi-key-store-test";

  @Rule public TemporaryFolder tempDir = new TemporaryFolder();

  @Test
  public void loadWithDefaultType() throws Exception {
    File store = newStore(KeyStore.getDefaultType());
    assertEquals(
        KeyStore.getDefaultType(), KeyStoreUtils.load(store.getPath(), null, PASSWORD).getType());
    assertEquals(
        KeyStore.getDefaultType(), KeyStoreUtils.load(store.getPath(), " ", PASSWORD).getType());
  }

  @Test
  public void loadWithGivenType() throws Exception {
    // a JCEKS store is loaded only with its type set, unlike the JKS and PKCS12 ones
    File store = newStore("JCEKS");
    assertEquals("JCEKS", KeyStoreUtils.load(store.getPath(), "JCEKS", PASSWORD).getType());
    assertThrows(IOException.class, () -> KeyStoreUtils.load(store.getPath(), null, PASSWORD));
  }

  @Test
  public void loadTrustStoreWithoutPassword() throws Exception {
    File store = newStore("JKS");
    assertEquals("JKS", KeyStoreUtils.load(store.getPath(), "JKS", null).getType());
  }

  @Test
  public void failOnInvalidStore() throws Exception {
    File store = newStore("JKS");
    assertThrows(IOException.class, () -> KeyStoreUtils.load(store.getPath(), "JKS", "wrong"));
    assertThrows(
        KeyStoreException.class, () -> KeyStoreUtils.load(store.getPath(), "unknown", PASSWORD));
    String missing = new File(tempDir.getRoot(), "missing.jks").getPath();
    assertThrows(NoSuchFileException.class, () -> KeyStoreUtils.load(missing, "JKS", PASSWORD));
  }

  private File newStore(String type) throws Exception {
    KeyStore store = KeyStore.getInstance(type);
    store.load(null, null);
    File file = tempDir.newFile();
    try (OutputStream out = Files.newOutputStream(file.toPath())) {
      store.store(out, PASSWORD.toCharArray());
    }
    return file;
  }
}
