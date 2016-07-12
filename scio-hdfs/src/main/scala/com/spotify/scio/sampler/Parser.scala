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

case class Config(inMode: String = "", in: String = "",
                  bqOut: String = "", fileOut: String = "",
                  n: Long = 1)

object Parser {

  // scalastyle:off if.brace
  val parser = new scopt.OptionParser[Config]("sampler") {
    head("sampler")

    opt[Long]('n', "numSamples")
      .action((x, c) => c.copy(n = x))
      .text("number of samples to collect")
      .required()

    cmd("bigquery")
      .action((_, c) => c.copy(inMode = "bigquery"))
      .text("Sample from BigQuery")
      .children(
        opt[String]("in").required()
          .action((x, c) => c.copy(in = x))
          .text("input BigQuery table"),
        opt[String]("bqOut")
          .action((x, c) => c.copy(bqOut = x))
          .text("output BigQuery table"),
        opt[String]("fileOut")
          .action((x, c) => c.copy(fileOut = x))
          .text("file output path"),
        checkConfig( c =>
          if (c.inMode == "bigquery" && c.bqOut.isEmpty && c.fileOut.isEmpty)
            failure("Missing output option")
          else success
        ))

    cmd("avro")
      .action((_, c) => c.copy(inMode = "avro"))
      .text("Sample from Avro")
      .children(
        opt[String]("in")
          .required()
          .action((x, c) => c.copy(in = x))
          .text("input Avro path"),
        opt[String]("fileOut")
          .required()
          .action((x, c) => c.copy(bqOut = x))
          .text("output Avro file"))


    checkConfig( c =>
      if (c.n <= 0) failure("n must be > 0")
      else if (c.inMode.isEmpty) failure("Missing command")
      else success)
  }
  // scalastyle:on if.brace

  def parse(args: Array[String]): Option[Config] = parser.parse(args, Config())

}
