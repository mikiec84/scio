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

import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

object Test {

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("Test")
    // val path = new Path("hdfs:///anonym/cleaned/endsong/2016-07-01/00")
    val path = new Path("gs://scio-example/anonym/cleaned/endsong/2016-03-01/00")
    val r = new AvroRandomSampler(path).sample(1000)
    r.map(_.toString).foreach(logger.info)
  }


}
