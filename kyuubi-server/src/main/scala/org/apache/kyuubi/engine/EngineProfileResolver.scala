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

package org.apache.kyuubi.engine

import java.util.Locale

import org.apache.kyuubi.{KyuubiSQLException, Logging, Utils}
import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

/**
 * Resolves the [[EngineProfile]] to apply to a session, if any, by consulting the precomputed
 * [[EngineProfileRegistry]] (which owns reading and materializing the profile declarations).
 *
 * Resolution precedence (high to low):
 *   1. the explicit session parameter `kyuubi.engine.profile`;
 *   2. the user/group default profile via the `___<principal>___.kyuubi.engine.profile` overlay
 *      (the session user wins, then groups in their provided order);
 *   3. the per-engine-type default `kyuubi.engine.<TYPE>.profile.default`, where `<TYPE>` is the
 *      engine type requested by the session, then a user/group default engine type, then the
 *      server-wide default engine type.
 *
 * When no profile resolves, `None` is returned and the legacy behavior is preserved. When a
 * profile name resolves but is not defined, the behavior is governed by
 * `kyuubi.engine.profiles.unknown.strategy`: `FAIL` (default) throws [[KyuubiSQLException]], while
 * `LOG` logs a warning and falls back to `None` (no profile applied).
 */
object EngineProfileResolver extends Logging {

  def resolve(
      serverConf: KyuubiConf,
      registry: EngineProfileRegistry,
      user: String,
      groups: Seq[String],
      requestConf: Map[String, String]): Option[EngineProfile] = {
    resolveProfileName(serverConf, user, groups, requestConf).flatMap { name =>
      registry.get(name) match {
        case Some(profile) =>
          warnOnEngineTypeMismatch(name, profile, requestConf)
          Some(profile)
        case None =>
          handleUndefinedProfile(serverConf, registry, name)
          None
      }
    }.fold[Option[EngineProfile]] {
      info(s"No engine profile resolved for user '$user', proceeding without one.")
      None
    } { profile =>
      info(s"Proceeding with engine profile '${profile.name}' for user '$user'.")
      Some(profile)
    }
  }

  private def resolveProfilesBlacklist(
      serverConf: KyuubiConf,
      user: String,
      groups: Seq[String]): Set[String] = {
    userAndGroupConfigs(serverConf, user, groups, ENGINE_PROFILES_BLACKLIST.key)
      .flatMap(Utils.strToSeq(_))
      .toSet
  }

  private def handleUndefinedProfile(
      serverConf: KyuubiConf,
      registry: EngineProfileRegistry,
      profileName: String): Unit = {
    val message =
      s"Engine profile '$profileName' is not defined. Available profiles:" +
        s" ${registry.names.toSeq.sorted.mkString("[", ", ", "]")}."
    serverConf.get(ENGINE_PROFILES_UNKNOWN_STRATEGY) match {
      case "LOG" =>
        warn(message)
      case _ =>
        throw KyuubiSQLException(message)
    }
  }

  private def warnOnEngineTypeMismatch(
      name: String,
      profile: EngineProfile,
      requestConf: Map[String, String]): Unit = {
    profile.conf.get(ENGINE_TYPE.key).foreach { profileType =>
      requestConf.get(ENGINE_TYPE.key).map(_.toUpperCase(Locale.ROOT)).foreach { requestType =>
        if (requestType != profileType) {
          warn(s"Session requested engine type '$requestType' but engine profile '$name'" +
            s" declares engine type '$profileType'; the session's requested engine type takes" +
            s" precedence.")
        }
      }
    }
  }

  private def resolveProfileName(
      serverConf: KyuubiConf,
      user: String,
      groups: Seq[String],
      requestConf: Map[String, String]): Option[String] = {
    val profilesBlacklist = resolveProfilesBlacklist(serverConf, user, groups)

    requestConf.get(ENGINE_PROFILE.key)
      .filter(_.nonEmpty)
      .map(requireNotBlacklisted(_, profilesBlacklist, user))
      .orElse(userAndGroupConfigs(serverConf, user, groups, ENGINE_PROFILE.key)
        .find(allowedImplicitly(_, profilesBlacklist, user)))
      .orElse(defaultProfile(serverConf, resolveEngineType(serverConf, user, groups, requestConf))
        .filter(allowedImplicitly(_, profilesBlacklist, user)))
  }

  /** The configured global default profile for an engine type, if any. */
  private def defaultProfile(serverConf: KyuubiConf, engineType: String): Option[String] = {
    ENGINE_DEFAULT_PROFILE.get(engineType.toUpperCase(Locale.ROOT)).flatMap(serverConf.get)
  }

  private def resolveEngineType(
      serverConf: KyuubiConf,
      user: String,
      groups: Seq[String],
      requestConf: Map[String, String]): String = {
    requestConf.get(ENGINE_TYPE.key).filter(_.nonEmpty)
      .orElse(userOrGroupDefault(serverConf, user, groups, ENGINE_TYPE.key))
      .getOrElse(serverConf.get(ENGINE_TYPE))
      .toUpperCase(Locale.ROOT)
  }

  /**
   * Look up a `___<principal>___.<key>` default value, checking the session user first and then
   * each group in the order provided.
   */
  private def userOrGroupDefault(
      serverConf: KyuubiConf,
      user: String,
      groups: Seq[String],
      key: String): Option[String] = {
    userAndGroupConfigs(serverConf, user, groups, key)
      .find(_.nonEmpty)
  }

  private def userAndGroupConfigs(
    serverConf: KyuubiConf,
    user: String,
    groups: Seq[String],
    key: String): Iterator[String] = {
    (user +: groups).iterator
      .flatMap { principal =>
        serverConf.getAllWithPrefix(
          s"$USER_DEFAULTS_CONF_QUOTE$principal$USER_DEFAULTS_CONF_QUOTE",
          "").get(key)
      }
      .filter(_.nonEmpty)
      .map(_.trim)
  }

  private def allowedImplicitly(
    profile: String,
    profilesBlacklist: Set[String],
    user: String): Boolean = {
    if (profilesBlacklist.contains(profile)) {
      warn(s"User '$user' is not allowed to implicitly use the engine profile '$profile', " +
        "skipping it.")
      false
    } else {
      true
    }
  }

  private def requireNotBlacklisted(
    profile: String,
    profilesBlacklist: Set[String],
    user: String): String = {
    if (profilesBlacklist.contains(profile)) {
      throw KyuubiSQLException(
        s"Current user '$user' is not allowed to use the engine profile '$profile'")
    }
    profile
  }
}
