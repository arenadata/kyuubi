/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kyuubi.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.kyuubi.client.exception.KyuubiRestException;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class HttpClientFactoryTest {

  private static final String PASSWORD = "kyuubi-password";

  @ClassRule public static final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private static File trustStore;
  private static File jceksTrustStore;
  private static File otherTrustStore;
  private static Server server;
  private static int port;

  @BeforeClass
  public static void setUp() throws Exception {
    // the server certificate is valid for localhost, but not for 127.0.0.1
    File keyStore = generateKeyStore("server");
    trustStore = generateTrustStore(keyStore, "server");
    jceksTrustStore = generateTrustStore(keyStore, "server", "JCEKS");
    otherTrustStore = generateTrustStore(generateKeyStore("other"), "other");

    SslContextFactory.Server sslContextFactory = new SslContextFactory.Server();
    sslContextFactory.setKeyStorePath(keyStore.getAbsolutePath());
    sslContextFactory.setKeyStorePassword(PASSWORD);
    server = new Server();
    ServerConnector connector = new ServerConnector(server, sslContextFactory);
    server.addConnector(connector);
    server.setHandler(
        new AbstractHandler() {
          @Override
          public void handle(
              String target,
              Request baseRequest,
              HttpServletRequest request,
              HttpServletResponse response)
              throws IOException {
            response.setContentType("text/plain");
            response.getWriter().print("pong");
            baseRequest.setHandled(true);
          }
        });
    server.start();
    port = connector.getLocalPort();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  public void testTrustAllCertificatesWithoutTrustStore() throws Exception {
    try (KyuubiRestClient client = newClient("127.0.0.1", null, null)) {
      assertEquals("pong", new BaseRestApi(client).ping());
    }
  }

  @Test
  public void testVerifyServerCertificateWithTrustStore() throws Exception {
    try (KyuubiRestClient client = newClient("localhost", trustStore, "JKS")) {
      assertEquals("pong", new BaseRestApi(client).ping());
    }
  }

  @Test
  public void testTrustStoreType() throws Exception {
    // a JCEKS truststore is loaded only with its type set, unlike the JKS and PKCS12 ones
    try (KyuubiRestClient client = newClient("localhost", jceksTrustStore, "JCEKS")) {
      assertEquals("pong", new BaseRestApi(client).ping());
    }
    assertBuildFails(jceksTrustStore, PASSWORD, IOException.class);
  }

  @Test
  public void testRejectUntrustedServerCertificate() throws Exception {
    try (KyuubiRestClient client = newClient("localhost", otherTrustStore, null)) {
      assertPingFails(client, SSLHandshakeException.class);
    }
  }

  @Test
  public void testRejectHostNameNotInServerCertificate() throws Exception {
    try (KyuubiRestClient client = newClient("127.0.0.1", trustStore, null)) {
      assertPingFails(client, SSLPeerUnverifiedException.class);
    }
  }

  @Test
  public void testClonedClientVerifiesServerCertificate() throws Exception {
    try (KyuubiRestClient client = newClient("localhost", otherTrustStore, null);
        KyuubiRestClient clonedClient = client.clone()) {
      clonedClient.setHostUrls("https://localhost:" + port);
      assertPingFails(clonedClient, SSLHandshakeException.class);
    }
  }

  @Test
  public void testInvalidTrustStore() {
    File missingTrustStore = new File(TEMP_DIR.getRoot(), "missing.jks");
    assertBuildFails(missingTrustStore, PASSWORD, NoSuchFileException.class);
    assertBuildFails(trustStore, "wrong-password", IOException.class);
  }

  private static KyuubiRestClient newClient(String host, File trustStore, String trustStoreType) {
    KyuubiRestClient.Builder builder =
        KyuubiRestClient.builder("https://" + host + ":" + port).maxAttempts(1);
    if (trustStore != null) {
      builder
          .trustStorePath(trustStore.getAbsolutePath())
          .trustStorePassword(PASSWORD)
          .trustStoreType(trustStoreType);
    }
    return builder.build();
  }

  private static void assertPingFails(
      KyuubiRestClient client, Class<? extends Throwable> expectedCause) {
    try {
      new BaseRestApi(client).ping();
      fail("The request should fail with " + expectedCause.getName());
    } catch (KyuubiRestException e) {
      assertTrue("Unexpected failure: " + e, hasCause(e, expectedCause));
    }
  }

  private static void assertBuildFails(
      File trustStore, String password, Class<? extends Throwable> expectedCause) {
    try {
      KyuubiRestClient.builder("https://localhost:" + port)
          .trustStorePath(trustStore.getAbsolutePath())
          .trustStorePassword(password)
          .build();
      fail("Building the client should fail with " + expectedCause.getName());
    } catch (KyuubiRestException e) {
      assertTrue("Unexpected failure: " + e, hasCause(e, expectedCause));
    }
  }

  private static boolean hasCause(Throwable e, Class<? extends Throwable> causeClass) {
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (causeClass.isInstance(t)) {
        return true;
      }
    }
    return false;
  }

  private static File generateKeyStore(String alias) throws Exception {
    File keyStore = new File(TEMP_DIR.getRoot(), alias + ".jks");
    keytool(
        "-genkeypair",
        "-noprompt",
        "-alias",
        alias,
        "-keyalg",
        "RSA",
        "-keysize",
        "2048",
        "-dname",
        "CN=localhost",
        "-ext",
        "SAN=dns:localhost",
        "-validity",
        "1",
        "-storetype",
        "JKS",
        "-keystore",
        keyStore.getAbsolutePath(),
        "-storepass",
        PASSWORD,
        "-keypass",
        PASSWORD);
    return keyStore;
  }

  private static File generateTrustStore(File keyStore, String alias) throws Exception {
    return generateTrustStore(keyStore, alias, "JKS");
  }

  private static File generateTrustStore(File keyStore, String alias, String storeType)
      throws Exception {
    File certificate = new File(TEMP_DIR.getRoot(), alias + ".pem");
    keytool(
        "-exportcert",
        "-rfc",
        "-alias",
        alias,
        "-keystore",
        keyStore.getAbsolutePath(),
        "-storepass",
        PASSWORD,
        "-file",
        certificate.getAbsolutePath());
    File trustStore =
        new File(TEMP_DIR.getRoot(), alias + "-truststore." + storeType.toLowerCase(Locale.ROOT));
    keytool(
        "-importcert",
        "-noprompt",
        "-alias",
        alias,
        "-file",
        certificate.getAbsolutePath(),
        "-storetype",
        storeType,
        "-keystore",
        trustStore.getAbsolutePath(),
        "-storepass",
        PASSWORD);
    return trustStore;
  }

  private static void keytool(String... args) throws Exception {
    List<String> command = new ArrayList<>();
    command.add(Paths.get(System.getProperty("java.home"), "bin", "keytool").toString());
    command.addAll(Arrays.asList(args));
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    try (InputStream in = process.getInputStream()) {
      byte[] buffer = new byte[4096];
      int read;
      while ((read = in.read(buffer)) != -1) {
        output.write(buffer, 0, read);
      }
    }
    if (process.waitFor() != 0) {
      throw new IllegalStateException(
          "keytool " + args[0] + " failed: " + output.toString(StandardCharsets.UTF_8.name()));
    }
  }
}
