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

package org.apache.kyuubi.plugin.kubernetes;

import org.apache.kyuubi.KyuubiSQLException;

/**
 * Throws {@link KyuubiSQLException} from where Java would not let it be declared.
 *
 * <p>The interfaces implemented here are Scala's, which has no checked exceptions, and their
 * callers expect a {@code KyuubiSQLException} - it is what the ZooKeeper client throws when a lock
 * times out, and what reaches the client as the reason a session did not open. Wrapping it in an
 * unchecked type would change what those callers see; this keeps it.
 */
final class Unchecked {

  private Unchecked() {}

  /** Never returns; declared as returning so a call can stand where a throw statement cannot. */
  static RuntimeException sqlException(String message, Throwable cause) {
    return sneaky(KyuubiSQLException.apply(message, cause, null, 0));
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> RuntimeException sneaky(Throwable t) throws T {
    throw (T) t;
  }
}
