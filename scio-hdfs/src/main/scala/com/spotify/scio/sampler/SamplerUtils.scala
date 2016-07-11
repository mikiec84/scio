/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.sampler

import org.apache.hadoop.fs.{Path, PathFilter}

import scala.util.Random

object SamplerUtils {

  /** Create a file extension [[PathFilter]]. */
  def extensionPathFilter(ext: String): PathFilter = new PathFilter {
    override def accept(path: Path): Boolean = path.getName.endsWith(ext)
  }

  /** Scale `weights` so that they sum up to `n`. */
  def scaleWeights(weights: Array[Long], n: Long): Array[Long] = {
    val sum = weights.sum
    require(sum > n, "sum of weights must be > n")
    val result = weights.map(x => (x.toDouble / sum * n).toLong)

    val delta = n - result.sum  // delta due to rounding error
    var i = 0
    val dim = weights.length
    while (i < delta) {
      // randomly distribute delta
      result(Random.nextInt(dim)) += 1
      i+= 1
    }

    result
  }

}
