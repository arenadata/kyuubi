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

package org.apache.kyuubi.server.flight

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Arrow 16's Netty buffer patch only accepts Netty 4.1 `PooledUnsafeDirectByteBuf`.
 * This build runs Netty 4.2, whose arena returns `PooledDirectByteBuf` (or an adaptive
 * buffer) and Arrow allocation then fails inside `NettyAllocationManager`.
 *
 * Flight SQL therefore pins the unshaded allocator to `arrow-memory-unsafe` before the
 * first `RootAllocator`. The system property is cleared afterwards: Spark Connect's
 * relocated Arrow reads the same property and has no Unsafe factory.
 */
object KyuubiFlightAllocator {

  private val installed = new AtomicBoolean(false)

  def useUnsafeAllocator(): Unit = synchronized {
    if (installed.get) {
      return
    }
    val property = "arrow.allocation.manager.type"
    val previous = System.getProperty(property)
    System.setProperty(property, "Unsafe")
    try {
      // BaseAllocator must be initialized before UnsafeAllocationManager, or factory
      // init deadlocks while ArrowBuf reads BaseAllocator.DEBUG.
      Class.forName("org.apache.arrow.memory.BaseAllocator")
      val optionClass = Class.forName("org.apache.arrow.memory.DefaultAllocationManagerOption")
      val factoryField = optionClass.getDeclaredField("DEFAULT_ALLOCATION_MANAGER_FACTORY")
      factoryField.setAccessible(true)
      if (factoryField.get(null) == null) {
        val getFactory = optionClass.getDeclaredMethod("getDefaultAllocationManagerFactory")
        getFactory.setAccessible(true)
        getFactory.invoke(null)
      }
      val unsafeFactory = Class
        .forName("org.apache.arrow.memory.unsafe.UnsafeAllocationManager")
        .getField("FACTORY")
        .get(null)
      factoryField.set(null, unsafeFactory)
      installed.set(true)
    } finally {
      if (previous == null) System.clearProperty(property)
      else System.setProperty(property, previous)
    }
  }
}
