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

package org.apache.kyuubi.gateway.security

import java.util.concurrent.TimeUnit

import scala.util.control.NonFatal

import org.apache.hadoop.security.UserGroupInformation

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.service.AbstractService
import org.apache.kyuubi.util.{KyuubiHadoopUtils, ThreadUtils}

/**
 * Logs the gateway in from its keytab, and keeps the ticket alive.
 *
 * Without this the SASL server has no server principal to offer. It falls back
 * to whichever OS user the process runs as and fails at startup with "Kerberos
 * principal should have 3 parts: nonroot" - which names the symptom and not the
 * cause, since nothing in that message suggests a missing login.
 *
 * Not Kyuubi's own `KinitAuxiliaryService`, which does the same in-process login
 * and then schedules the external `kinit` binary to renew, exiting the process
 * after a few failures. There is no `kinit` in a distroless image, so that path
 * would take the gateway down a minute after it started. Hadoop can renew
 * without leaving the JVM, and `checkTGTAndReloginFromKeytab` is how.
 */
class KerberosLogin extends AbstractService("KerberosLogin") {

  private val renewer = ThreadUtils.newDaemonSingleThreadScheduledExecutor(getName)
  private var interval: Long = _

  override def initialize(conf: KyuubiConf): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      val principal = conf.get(KyuubiConf.SERVER_PRINCIPAL)
        .map(KyuubiHadoopUtils.getServerPrincipal)
      val keytab = conf.get(KyuubiConf.SERVER_KEYTAB)
      require(
        principal.nonEmpty && keytab.nonEmpty,
        s"${KyuubiConf.SERVER_PRINCIPAL.key} and ${KyuubiConf.SERVER_KEYTAB.key} are both " +
          "required when Kerberos authentication is on")
      interval = conf.get(KyuubiConf.KINIT_INTERVAL)

      // Fails the startup rather than the first client. A gateway that came up
      // unable to authenticate anybody would report healthy and refuse every
      // session, which is harder to diagnose than not coming up.
      UserGroupInformation.loginUserFromKeytab(principal.get, keytab.get)
      info(s"Logged in as ${UserGroupInformation.getLoginUser.getUserName} from ${keytab.get}")
    } else {
      info("Hadoop security is off, the gateway will not log in from a keytab")
    }
    super.initialize(conf)
  }

  override def start(): Unit = {
    if (UserGroupInformation.isSecurityEnabled) {
      renewer.scheduleWithFixedDelay(
        () =>
          try {
            // A no-op until the ticket is close to expiring, so the interval
            // decides how late a renewal may be rather than how often one
            // happens.
            UserGroupInformation.getLoginUser.checkTGTAndReloginFromKeytab()
          } catch {
            case NonFatal(e) =>
              // Logged and retried, not fatal: the current ticket is still
              // valid for a while, and a KDC that is briefly unreachable must
              // not take the gateway down with it.
              warn("Could not renew the Kerberos ticket, will try again", e)
          },
        interval,
        interval,
        TimeUnit.MILLISECONDS)
    }
    super.start()
  }

  override def stop(): Unit = {
    ThreadUtils.shutdown(renewer)
    super.stop()
  }
}
