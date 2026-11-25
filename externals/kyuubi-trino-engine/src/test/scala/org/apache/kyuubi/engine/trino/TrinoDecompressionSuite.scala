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

package org.apache.kyuubi.engine.trino

import java.nio.charset.StandardCharsets.UTF_8
import java.util.Arrays

import io.airlift.compress.Compressor
import io.airlift.compress.lz4.Lz4Compressor
import io.airlift.compress.zstd.ZstdCompressor
import io.trino.client.spooling.encoding.DecompressionUtils

import org.apache.kyuubi.KyuubiFunSuite

class TrinoDecompressionSuite extends KyuubiFunSuite {

  private val payload = ("Kyuubi Trino compressed results: row 123\n" * 100).getBytes(UTF_8)

  private def checkCodec(
      compressor: Compressor,
      decompress: (Array[Byte], Array[Byte]) => Int,
      nativeClass: String): Unit = {
    // The optional native implementation must stay on the classpath. On JDK 17 its
    // newer bytecode cannot load, so successful decoding verifies Trino's Java fallback.
    assert(getClass.getClassLoader.getResource(nativeClass.replace('.', '/') + ".class") != null)
    val buffer = new Array[Byte](compressor.maxCompressedLength(payload.length))
    val length = compressor.compress(payload, 0, payload.length, buffer, 0, buffer.length)
    val compressed = Arrays.copyOf(buffer, length)
    val output = new Array[Byte](payload.length)
    assert(decompress(compressed, output) === payload.length)
    assert(output.toSeq === payload.toSeq)
  }

  test("decode LZ4 results with the optional native codec present") {
    checkCodec(
      new Lz4Compressor,
      DecompressionUtils.decompressLZ4,
      "io.airlift.compress.v3.lz4.Lz4NativeDecompressor")
  }

  test("decode Zstd results with the optional native codec present") {
    checkCodec(
      new ZstdCompressor,
      DecompressionUtils.decompressZstd,
      "io.airlift.compress.v3.zstd.ZstdNativeDecompressor")
  }
}
