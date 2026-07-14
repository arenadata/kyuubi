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

package org.apache.kyuubi.spark.connect.client

import java.security.PrivilegedExceptionAction
import java.util.Base64
import javax.security.auth.Subject
import javax.security.auth.login.{AppConfigurationEntry, Configuration, LoginContext}

import io.grpc.{ManagedChannel, Metadata}
import io.grpc.netty.NettyChannelBuilder
import io.grpc.stub.{MetadataUtils => GrpcMetadataUtils}
import org.apache.spark.sql.kyuubi.KyuubiAuthType
import org.ietf.jgss.{GSSContext, GSSManager, GSSName, Oid}

import org.apache.kyuubi.server.grpc.proto.{GetTokenRequest, RenewTokenRequest, RevokeTokenRequest, SparkConnectAuthServiceGrpc}

class KyuubiTokenClient(initHost: String, initPort: Int, ssl: Boolean = true) {

  // host/port can be retargeted by FailoverManagedChannel (failover thread) while renewToken()
  // may run on another thread, so these need the same visibility guarantee as the channel's fields.
  @volatile private var host: String = initHost
  @volatile private var port: Int = initPort

  private var token: String = _
  private var expiresAtMs: Long = _

  def getToken(auth: KyuubiAuthType, username: String = null, password: String = null): String = {
    val authHeader = auth match {
      case KyuubiAuthType.KERBEROS =>
        s"Negotiate ${spnegoToken()}"
      case KyuubiAuthType.LDAP =>
        require(
          username != null && password != null,
          "username and password are required for LDAP auth")
        val encoded = Base64.getEncoder.encodeToString(s"$username:$password".getBytes("UTF-8"))
        s"Basic $encoded"
      case other =>
        throw new IllegalArgumentException(
          s"Unknown auth type: $other")
    }

    val channel = buildChannel()
    try {
      val metadata = new Metadata()
      metadata.put(
        Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER),
        authHeader)
      val stub = SparkConnectAuthServiceGrpc.newBlockingStub(channel)
        .withInterceptors(GrpcMetadataUtils.newAttachHeadersInterceptor(metadata))
      val resp = stub.getToken(GetTokenRequest.newBuilder().build())
      token = resp.getToken
      expiresAtMs = resp.getExpiresAtMs
      token
    } finally {
      channel.shutdownNow()
    }
  }

  def renewToken(): Unit = {
    val channel = buildChannel()
    try {
      val resp = SparkConnectAuthServiceGrpc.newBlockingStub(channel)
        .renewToken(RenewTokenRequest.newBuilder().setToken(token).build())
      expiresAtMs = resp.getExpiresAtMs
    } finally {
      channel.shutdownNow()
    }
  }

  def revokeToken(): Unit = {
    val channel = buildChannel()
    try {
      SparkConnectAuthServiceGrpc.newBlockingStub(channel)
        .revokeToken(RevokeTokenRequest.newBuilder().setToken(token).build())
      token = null
    } finally {
      channel.shutdownNow()
    }
  }

  def currentToken: String = token

  def tokenExpiresAtMs: Long = expiresAtMs

  /**
   * Redirect subsequent token RPCs to a new server. Called by FailoverManagedChannel after it
   * fails over to a live server, so that renew/revoke calls keep reaching a reachable server.
   * Public because FailoverManagedChannel lives in a different package tree
   * (org.apache.spark.sql.kyuubi) that no cross-package private modifier can span from here.
   */
  def retarget(newHost: String, newPort: Int): Unit = {
    host = newHost
    port = newPort
  }

  def hostPort: (String, Int) = (host, port)

  private def buildChannel(): ManagedChannel = {
    val builder = NettyChannelBuilder.forAddress(host, port)
    if (ssl) builder.useTransportSecurity() else builder.usePlaintext()
    builder.build()
  }

  private def spnegoToken(): String = {
    val subject = loginFromTicketCache()
    Subject.doAs(
      subject,
      new PrivilegedExceptionAction[String] {
        override def run(): String = {
          val mechOid = new Oid("1.2.840.113554.1.2.2")
          val manager = GSSManager.getInstance()
          val serverName = manager.createName(s"HTTP@$host", GSSName.NT_HOSTBASED_SERVICE)
          val context =
            manager.createContext(serverName, mechOid, null, GSSContext.DEFAULT_LIFETIME)
          context.requestMutualAuth(false)
          val outToken = context.initSecContext(new Array[Byte](0), 0, 0)
          context.dispose()
          Base64.getEncoder.encodeToString(outToken)
        }
      })
  }

  private def loginFromTicketCache(): Subject = {
    val options = new java.util.HashMap[String, String]()
    options.put("useTicketCache", "true")
    options.put("renewTGT", "true")
    options.put("doNotPrompt", "true")

    val config = new Configuration {
      override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] =
        Array(new AppConfigurationEntry(
          "com.sun.security.auth.module.Krb5LoginModule",
          AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
          options))
    }
    val loginContext = new LoginContext("", null, null, config)
    loginContext.login()
    loginContext.getSubject
  }
}
