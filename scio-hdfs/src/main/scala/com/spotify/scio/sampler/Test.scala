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

//  val p = "hdfs://hadoopha/anonym/cleaned/endsong/2016-07-01/00/part-r-00001.avro"
//  val p = "gs://scio-example/anonym/cleaned/endsong/2016-03-01/00"
  val p = "gs://scio-example/anonym/cleaned/endsong/2016-03-01/00/part-r-00001.avro"

  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger("Test")

//    val path = new Path(p)
//    val data = new AvroRandomSampler(path).sample(10)
//    new AvroDataWriter(new Path("sample.avro")).write(data)

    val opts = Parser.parse(args)
    if (opts.isEmpty) {
      sys.exit(-1)
    }

    val o = opts.get
    o.inMode match {
      case "avro" =>
        val data = new AvroRandomSampler(new Path(o.in)).sample(o.n)
        new AvroDataWriter(new Path(o.fileOut)).write(data)
      case _ =>
        throw new NotImplementedError(s"${o.inMode} not implemented")
    }
  }

}
