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

import scala.collection.mutable

import org.apache.kyuubi.config.KyuubiConf
import org.apache.kyuubi.config.KyuubiConf._

/**
 * An immutable registry of all [[EngineProfile]]s declared under `kyuubi.engine.profile.<name>.*`.
 *
 * This owns the engine-profile configuration logic: reading the profile declarations from a
 * [[KyuubiConf]] and materializing them into [[EngineProfile]]s. Engine profiles are loaded from
 * `kyuubi-defaults.conf` at startup and never change at runtime, so [[EngineProfileRegistry.apply]]
 * materializes them once (validating each) and the result is reused for every session open and
 * REST listing, rather than being re-parsed each time.
 */
class EngineProfileRegistry private (profiles: Map[String, EngineProfile]) {

  /** Names of all declared engine profiles. */
  def names: Set[String] = profiles.keySet

  /** The materialized profile for `name`, or None if no such profile is declared. */
  def get(name: String): Option[EngineProfile] = profiles.get(name)
}

object EngineProfileRegistry {

  // The profile declaration prefix, e.g. `kyuubi.engine.profile`.
  private val PROFILE_PREFIX = ENGINE_PROFILE.key

  /**
   * Materialize all engine profiles declared in `conf` into an immutable registry. Fails fast with
   * [[IllegalArgumentException]] if any profile has an invalid engine type or unknown bucket.
   */
  def apply(conf: KyuubiConf): EngineProfileRegistry = {
    val profiles = profileNames(conf)
      .map(name => name -> buildProfile(conf, name))
      .toMap
    new EngineProfileRegistry(profiles)
  }

  /** Names of all engine profiles declared under `kyuubi.engine.profile.<name>.*`. */
  private def profileNames(conf: KyuubiConf): Set[String] = {
    val prefix = s"$PROFILE_PREFIX."
    conf.getAll.keys.collect {
      case k if k.startsWith(prefix) => k.stripPrefix(prefix).split("\\.", 2).head
    }.toSet
  }

  /**
   * Materialize one named engine profile into effective engine-conf keys. Fails fast with
   * [[IllegalArgumentException]] on an invalid engine type or an unknown profile bucket.
   *
   * Bucket mapping for a `kyuubi.engine.profile.<name>.<bucket>[.<rest>]` key:
   *   - `type`           -> `kyuubi.engine.type` (validated against [[EngineType]])
   *   - `env.<VAR>`      -> `kyuubi.engineEnv.<VAR>`
   *   - `session.<rest>` -> `kyuubi.session.<rest>` (Kyuubi session variable)
   *   - `conf.<rest>`    -> `<rest>` verbatim (engine-native property)
   *
   * All materialized keys are applied to a session as defaults; conflicting client conf wins.
   */
  private def buildProfile(conf: KyuubiConf, name: String): EngineProfile = {
    val raw = conf.getAllWithPrefix(s"$PROFILE_PREFIX.$name", "")
    val profileConf = mutable.Map[String, String]()
    raw.foreach { case (suffix, value) =>
      val parts = suffix.split("\\.", 2)
      parts.head match {
        case "type" =>
          require(
            parts.length == 1,
            s"Invalid engine profile key '$PROFILE_PREFIX.$name.$suffix':" +
              s" 'type' does not accept a sub-key.")
          val engineType = value.toUpperCase(Locale.ROOT)
          require(
            EngineType.values.exists(_.toString == engineType),
            s"Invalid engine type '$value' in profile '$name', expected one of" +
              s" ${EngineType.values.mkString("[", ", ", "]")}.")
          profileConf += (ENGINE_TYPE.key -> engineType)
        case "env" =>
          require(
            parts.length == 2,
            s"Invalid engine profile key '$PROFILE_PREFIX.$name.$suffix':" +
              s" 'env' requires an environment variable name, e.g. env.SPARK_HOME.")
          profileConf += (s"$KYUUBI_ENGINE_ENV_PREFIX.${parts(1)}" -> value)
        case "session" =>
          require(
            parts.length == 2,
            s"Invalid engine profile key '$PROFILE_PREFIX.$name.$suffix':" +
              s" 'session' requires a Kyuubi session config key.")
          profileConf += (s"kyuubi.session.${parts(1)}" -> value)
        case "conf" =>
          require(
            parts.length == 2,
            s"Invalid engine profile key '$PROFILE_PREFIX.$name.$suffix':" +
              s" 'conf' requires an engine config key.")
          profileConf += (parts(1) -> value)
        case bucket =>
          throw new IllegalArgumentException(
            s"Unknown engine profile bucket '$bucket' in key '$PROFILE_PREFIX.$name.$suffix'." +
              s" Expected one of: type, env.<VAR>, session.<key>, conf.<key>.")
      }
    }
    EngineProfile(name, profileConf.toMap)
  }
}
